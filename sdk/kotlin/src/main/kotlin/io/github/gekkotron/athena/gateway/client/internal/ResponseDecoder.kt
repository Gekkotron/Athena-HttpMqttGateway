package io.github.gekkotron.athena.gateway.client.internal

import io.github.gekkotron.athena.gateway.client.GatewayException
import io.github.gekkotron.athena.gateway.client.GatewayResponse
import io.github.gekkotron.athena.gateway.client.MqttEvent
import io.github.gekkotron.athena.gateway.client.PublishResult
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.booleanOrNull
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.intOrNull
import kotlinx.serialization.json.longOrNull
import okio.ByteString.Companion.decodeBase64

/** What the transport got back: status code, content type, and body text. */
internal data class RawResponse(val code: Int, val contentType: String?, val body: String)

/** One decoded SSE frame of `/mqtt/subscribe`. */
internal sealed interface Frame {
    data class Event(val event: MqttEvent) : Frame
    data class Error(val error: GatewayException) : Frame
    data object Disconnected : Frame
    data object Ignored : Frame
}

internal class ResponseDecoder(private val crypto: WireCrypto) {

    private class Envelope(val status: Int, val body: JsonElement)

    fun http(raw: RawResponse, throwOnUpstreamError: Boolean): GatewayResponse {
        val env = envelope(raw)
        if (env.status !in 200..299 && throwOnUpstreamError) throw GatewayException.Upstream(env.status, env.body)
        return GatewayResponse(env.status, env.body)
    }

    fun publish(raw: RawResponse): PublishResult {
        val env = envelope(raw)
        val body = env.body.parsedIfJsonString()
        val obj = body as? JsonObject
        val topic = obj.string("topic")
        if (env.status != 200 || obj?.get("success").boolean() != true || topic == null) {
            throw GatewayException.Upstream(env.status, body)
        }
        return PublishResult(topic)
    }

    /** `/mqtt/subscribe` answered without an event stream: always a failure. */
    fun streamRejection(raw: RawResponse): Nothing {
        val env = envelope(raw)
        throw GatewayException.GatewayError("Expected an event stream, got status ${env.status}")
    }

    fun frame(data: String): Frame {
        // A corrupt frame says nothing about the next attempt, so let `reconnect` retry it.
        val obj = open(data, transient = true)
        val raw = obj.string("payload_b64")?.decodeBase64()?.toByteArray()
        return when (obj.string("type")) {
            "connected" -> Frame.Event(MqttEvent.Connected(
                (obj["topics"] as? JsonArray)?.mapNotNull { (it as? JsonPrimitive)?.contentOrNull }
                    ?: listOfNotNull(obj.string("topic")),
            ))
            "message" -> Frame.Event(MqttEvent.Message(
                topic = obj.string("topic").orEmpty(),
                payload = raw?.let(::payloadOf) ?: obj["payload"] ?: JsonNull,
                qos = (obj["qos"] as? JsonPrimitive)?.intOrNull ?: 0,
                retain = obj["retain"].boolean() ?: false,
                timestamp = (obj["timestamp"] as? JsonPrimitive)?.longOrNull ?: 0L,
                raw = raw,
            ))
            "error" -> Frame.Error(streamError(obj))
            "disconnected" -> Frame.Disconnected
            else -> Frame.Ignored
        }
    }

    private fun envelope(raw: RawResponse): Envelope {
        when (raw.code) {
            200 -> Unit
            401 -> throw GatewayException.Unauthorized()
            400 -> throw GatewayException.BadRequest()
            in RETRYABLE_PLAINTEXT_STATUSES -> throw GatewayException.GatewayError("HTTP ${raw.code}", transient = true)
            else -> throw GatewayException.GatewayError("HTTP ${raw.code}")
        }
        val obj = open(raw.body)
        val status = (obj["status"] as? JsonPrimitive)?.intOrNull
            ?: throw GatewayException.GatewayError("Response has no status")
        val body = obj["body"] ?: JsonNull
        if (obj.string("source") == "gateway") {
            val reason = body.parsedIfJsonString().errorMessage()
            throw if (status == 403) GatewayException.Rejected(reason) else GatewayException.GatewayError(reason)
        }
        return Envelope(status, body)
    }

    private fun streamError(obj: JsonObject): GatewayException {
        val reason = obj.string("message") ?: "stream error"
        val code = (obj["code"] as? JsonPrimitive)?.intOrNull
        return if (code in BROKER_LOGIN_REFUSED) GatewayException.BrokerRefused(code!!, reason)
        else GatewayException.StreamError(reason)
    }

    private fun open(data: String, transient: Boolean = false): JsonObject = try {
        crypto.open(data)
    } catch (e: Exception) {
        throw GatewayException.GatewayError("Response could not be decrypted", transient)
    }

    private companion object {
        /** Plaintext (pre-decrypt) statuses a proxy or gateway restart can produce; safe to retry. */
        val RETRYABLE_PLAINTEXT_STATUSES = setOf(408, 429, 502, 503, 504)
    }
}

private fun JsonObject?.string(key: String): String? = (this?.get(key) as? JsonPrimitive)?.contentOrNull

private fun JsonElement?.boolean(): Boolean? = (this as? JsonPrimitive)?.booleanOrNull

/** The gateway JSON-encodes some bodies as a string; decode those, leave everything else alone. */
private fun JsonElement.parsedIfJsonString(): JsonElement =
    if (this is JsonPrimitive && isString) runCatching { Json.parseToJsonElement(content) }.getOrDefault(this) else this

private fun JsonElement.errorMessage(): String =
    ((this as? JsonObject)?.get("error") as? JsonPrimitive)?.contentOrNull ?: toString()

/** MQTT CONNACK codes 4 (bad username or password) and 5 (not authorized). */
private val BROKER_LOGIN_REFUSED = setOf(4, 5)

/** UTF-8 JSON keeps its literals; other UTF-8 text becomes a string; anything else is null. */
private fun payloadOf(raw: ByteArray): JsonElement {
    val text = try {
        raw.decodeToString(throwOnInvalidSequence = true)
    } catch (e: CharacterCodingException) {
        return JsonNull
    }
    return runCatching { Json.parseToJsonElement(text) }.getOrElse { JsonPrimitive(text) }
}
