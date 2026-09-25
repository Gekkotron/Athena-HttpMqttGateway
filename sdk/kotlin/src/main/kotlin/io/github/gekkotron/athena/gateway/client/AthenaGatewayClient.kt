package io.github.gekkotron.athena.gateway.client

import io.github.gekkotron.athena.gateway.client.internal.Frame
import io.github.gekkotron.athena.gateway.client.internal.OkHttpTransport
import io.github.gekkotron.athena.gateway.client.internal.RequestBuilder
import io.github.gekkotron.athena.gateway.client.internal.ResponseDecoder
import io.github.gekkotron.athena.gateway.client.internal.StreamItem
import io.github.gekkotron.athena.gateway.client.internal.Transport
import io.github.gekkotron.athena.gateway.client.internal.WireCrypto
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.emitAll
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.flow.onCompletion
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.flow.retryWhen
import kotlinx.coroutines.flow.transformWhile
import kotlinx.coroutines.withContext
import kotlinx.serialization.json.JsonElement
import okhttp3.OkHttpClient

/**
 * Client for an Athena HTTP/MQTT gateway.
 *
 * Thread-safe; create one instance and share it.
 */
public class AthenaGatewayClient internal constructor(
    private val transport: Transport,
    crypto: WireCrypto,
    private val decodeDispatcher: CoroutineDispatcher = Dispatchers.Default,
    clock: () -> Long,
) {
    /**
     * @param baseUrl gateway root, e.g. `https://myhost.tailnet-name.ts.net` (a path prefix is allowed).
     * @param secretKey the 64-hex-character secret configured on the gateway.
     * @param okHttpClient bring your own client for certificate pinning, proxies or timeouts.
     * @param clock unix seconds used for request timestamps; override to correct a skewed device
     *   clock (the gateway rejects requests more than 60 s off).
     * @throws IllegalArgumentException if [baseUrl] or [secretKey] is malformed.
     */
    public constructor(
        baseUrl: String,
        secretKey: String,
        okHttpClient: OkHttpClient = OkHttpClient(),
        clock: () -> Long = { System.currentTimeMillis() / 1000 },
    ) : this(OkHttpTransport(baseUrl, okHttpClient), WireCrypto.fromHex(secretKey), clock = clock)

    private val requests = RequestBuilder(crypto, clock)
    private val decoder = ResponseDecoder(crypto)

    /**
     * Forwards an HTTP request to [url] through the gateway.
     *
     * @param url the destination the gateway should forward this request to.
     * @param method the HTTP method to forward.
     * @param headers extra headers to forward.
     * @param body a `JsonObject` (sent as JSON) or a JSON string (sent raw).
     * @param timeoutSeconds how long the gateway should wait on the upstream call; the client's own
     *   read timeout for this call is raised to accommodate it.
     * @param throwOnUpstreamError when false, non-2xx upstream answers are returned instead of thrown.
     * @throws GatewayException
     */
    public suspend fun http(
        url: String,
        method: String = "GET",
        headers: Map<String, String> = emptyMap(),
        body: JsonElement? = null,
        timeoutSeconds: Int? = null,
        throwOnUpstreamError: Boolean = true,
    ): GatewayResponse {
        val raw = transport.post(
            "gateway",
            requests.http(url, method, headers, body, timeoutSeconds),
            readTimeoutSeconds = (timeoutSeconds ?: 30) + 15,
        )
        return withContext(decodeDispatcher) { decoder.http(raw, throwOnUpstreamError) }
    }

    /**
     * Publishes [message] to [topic].
     *
     * @param topic the MQTT topic to publish to.
     * @param message the payload to publish.
     * @param qos the MQTT QoS to publish with (0, 1 or 2).
     * @param retain whether the broker should retain this message.
     * @param broker per-call broker overrides; `null` uses the gateway's defaults.
     * @throws GatewayException
     */
    public suspend fun publish(
        topic: String,
        message: String,
        qos: Int = 0,
        retain: Boolean = false,
        broker: Broker? = null,
    ): PublishResult {
        val raw = transport.post("mqtt/publish", requests.publish(topic, message, qos, retain, broker))
        return withContext(decodeDispatcher) { decoder.publish(raw) }
    }

    /**
     * Streams MQTT [topics] live. Cold: collection opens the stream, cancellation closes it.
     *
     * Without [reconnect] the flow completes when the gateway reports a disconnect and fails on
     * any error. With [reconnect] it re-opens the stream after disconnects, stream errors and
     * network failures, waiting per [Backoff]; `Unauthorized`, `BadRequest` and `Rejected` are
     * never retried. Every attempt sends a newly encrypted request.
     *
     * @throws IllegalArgumentException if no topic is given, a topic is blank, or [qos] is not 0, 1 or 2.
     */
    public fun subscribe(
        vararg topics: String,
        qos: Int = 0,
        broker: Broker? = null,
        reconnect: Backoff? = null,
    ): Flow<MqttEvent> {
        require(topics.isNotEmpty()) { "subscribe needs at least one topic" }
        require(topics.all { it.isNotBlank() }) { "topic must not be blank" }
        require(qos in 0..2) { "qos must be 0, 1 or 2" }
        val once = streamOnce(topics.toList(), qos, broker)
        val backoff = reconnect ?: return once
        return flow {
            var failures = 0
            emitAll(
                once
                    .onEach { if (it is MqttEvent.Connected) failures = 0 }
                    .onCompletion { cause -> if (cause == null) throw StreamEnded }
                    .retryWhen { cause, _ ->
                        val retry = cause === StreamEnded || (cause is GatewayException && cause.retryable)
                        if (retry) {
                            val wait = backoff.delayFor(failures++)
                            emit(MqttEvent.Reconnecting(cause.takeIf { it !== StreamEnded }, wait))
                            delay(wait)
                        }
                        retry
                    },
            )
        }
    }

    /** One connection attempt; the request is built at collection time so it is never reused. */
    private fun streamOnce(topics: List<String>, qos: Int, broker: Broker?): Flow<MqttEvent> = flow {
        val body = requests.subscribe(topics, qos, broker)
        emitAll(
            transport.stream("mqtt/subscribe", body).transformWhile { item ->
                when (item) {
                    is StreamItem.NotAStream -> decoder.streamRejection(item.response)
                    is StreamItem.Data -> when (val frame = decoder.frame(item.data)) {
                        is Frame.Event -> { emit(frame.event); true }
                        is Frame.Error -> throw GatewayException.StreamError(frame.reason)
                        Frame.Disconnected -> false
                        Frame.Ignored -> true
                    }
                }
            }.flowOn(decodeDispatcher),
        )
    }

    /** Internal signal: a stream ended normally and should be re-opened. Never escapes. */
    private object StreamEnded : Exception()
}
