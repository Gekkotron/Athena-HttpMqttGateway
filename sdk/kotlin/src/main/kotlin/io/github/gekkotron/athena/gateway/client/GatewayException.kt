package io.github.gekkotron.athena.gateway.client

import java.io.IOException
import kotlinx.serialization.json.JsonElement

/** Every failure surfaced by [AthenaGatewayClient]. */
public sealed class GatewayException(message: String, cause: Throwable? = null) : Exception(message, cause) {
    internal open val retryable: Boolean get() = false

    /** HTTP 401: no secret on the gateway matched this key. */
    public class Unauthorized : GatewayException("No gateway secret matched this key")

    /** HTTP 400: the gateway could not parse the request. */
    public class BadRequest : GatewayException("Gateway could not parse the request")

    /** The gateway refused the request (expired, replayed, port or destination not allowed). */
    public class Rejected(public val reason: String) : GatewayException(reason)

    /** The gateway failed internally, or answered with something unexpected. */
    public class GatewayError(public val reason: String) : GatewayException(reason)

    /** The target service (HTTP device or MQTT broker) returned a failure. */
    public class Upstream(public val status: Int, public val body: JsonElement) :
        GatewayException("Upstream returned $status")

    /** The subscription stream reported an error. */
    public class StreamError(public val reason: String) : GatewayException(reason) {
        override val retryable: Boolean get() = true
    }

    /** Network failure talking to the gateway. */
    public class Transport(cause: IOException) : GatewayException(cause.message ?: "I/O error", cause) {
        override val retryable: Boolean get() = true
    }
}
