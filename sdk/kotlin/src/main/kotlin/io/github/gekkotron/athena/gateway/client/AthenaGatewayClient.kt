package io.github.gekkotron.athena.gateway.client

import io.github.gekkotron.athena.gateway.client.internal.OkHttpTransport
import io.github.gekkotron.athena.gateway.client.internal.RequestBuilder
import io.github.gekkotron.athena.gateway.client.internal.ResponseDecoder
import io.github.gekkotron.athena.gateway.client.internal.Transport
import io.github.gekkotron.athena.gateway.client.internal.WireCrypto
import kotlinx.serialization.json.JsonElement
import okhttp3.OkHttpClient

/**
 * Client for an Athena HTTP/MQTT gateway.
 *
 * @param baseUrl gateway root, e.g. `https://myhost.tailnet-name.ts.net` (a path prefix is allowed).
 * @param secretKey the 64-hex-character secret configured on the gateway.
 * @param okHttpClient bring your own client for certificate pinning, proxies or timeouts.
 * @param clock unix seconds used for request timestamps; override only in tests.
 * @throws IllegalArgumentException if [baseUrl] or [secretKey] is malformed.
 */
public class AthenaGatewayClient internal constructor(
    private val transport: Transport,
    crypto: WireCrypto,
    clock: () -> Long,
) {
    public constructor(
        baseUrl: String,
        secretKey: String,
        okHttpClient: OkHttpClient = OkHttpClient(),
        clock: () -> Long = { System.currentTimeMillis() / 1000 },
    ) : this(OkHttpTransport(baseUrl, okHttpClient), WireCrypto.fromHex(secretKey), clock)

    private val requests = RequestBuilder(crypto, clock)
    private val decoder = ResponseDecoder(crypto)

    /**
     * Forwards an HTTP request to [url] through the gateway.
     *
     * @param body a `JsonObject` (sent as JSON) or a JSON string (sent raw).
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
    ): GatewayResponse = decoder.http(
        transport.post("gateway", requests.http(url, method, headers, body, timeoutSeconds)),
        throwOnUpstreamError,
    )

    /**
     * Publishes [message] to [topic].
     *
     * @throws GatewayException
     */
    public suspend fun publish(
        topic: String,
        message: String,
        qos: Int = 0,
        retain: Boolean = false,
        broker: Broker? = null,
    ): PublishResult = decoder.publish(
        transport.post("mqtt/publish", requests.publish(topic, message, qos, retain, broker)),
    )
}
