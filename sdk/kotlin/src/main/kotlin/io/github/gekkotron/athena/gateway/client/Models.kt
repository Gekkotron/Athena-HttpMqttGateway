package io.github.gekkotron.athena.gateway.client

import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.decodeFromJsonElement

/** Lenient decoder used by [GatewayResponse.bodyAs]. */
@PublishedApi
internal val GatewayJson: Json = Json { ignoreUnknownKeys = true }

/** Response of a request forwarded through `/gateway`. */
public data class GatewayResponse(public val status: Int, public val body: JsonElement) {
    /** Decodes [body] into a `@Serializable` type. */
    public inline fun <reified T> bodyAs(json: Json = GatewayJson): T = json.decodeFromJsonElement(body)
}

/** A successful MQTT publish. */
public data class PublishResult(public val topic: String)

/** Per-call MQTT broker overrides; `null` fields use the gateway's defaults. */
public data class Broker(
    public val host: String? = null,
    public val port: Int? = null,
    public val username: String? = null,
    public val password: String? = null,
) {
    /** Never prints [password]: renders `password=***` when it is set. */
    override fun toString(): String =
        "Broker(host=$host, port=$port, username=$username, password=${if (password != null) "***" else null})"
}

/** Exponential reconnect delay for [AthenaGatewayClient.subscribe]. */
public data class Backoff(
    public val initial: Duration = 1.seconds,
    public val max: Duration = 60.seconds,
    public val factor: Double = 2.0,
) {
    init {
        require(initial.isPositive()) { "initial must be > 0" }
        require(max >= initial) { "max must be >= initial" }
        require(factor >= 1.0) { "factor must be >= 1" }
    }

    internal fun delayFor(attempt: Int): Duration {
        var d = initial
        repeat(attempt) {
            d *= factor
            if (d >= max) return max
        }
        return d
    }
}

/**
 * Events emitted by [AthenaGatewayClient.subscribe].
 *
 * New event types may be added in minor releases; include an `else` branch when matching.
 */
public sealed interface MqttEvent {
    /** The gateway connected to the broker and subscribed to [topics]. */
    public data class Connected(public val topics: List<String>) : MqttEvent

    /** A message on [topic]; [payload] is parsed JSON, or a JSON string for text payloads. */
    public data class Message(
        public val topic: String,
        public val payload: JsonElement,
        public val qos: Int,
        public val retain: Boolean,
        public val timestamp: Long,
    ) : MqttEvent

    /**
     * Emitted only when `reconnect` is set, right before waiting [delay] to re-open the stream.
     * [cause] is null when the stream ended normally via `disconnected`, or the failure otherwise.
     */
    public data class Reconnecting(public val cause: Throwable?, public val delay: Duration) : MqttEvent
}
