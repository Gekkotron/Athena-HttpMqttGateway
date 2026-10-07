package io.github.gekkotron.athena.gateway.client.internal

import io.github.gekkotron.athena.gateway.client.Broker
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive

/** Builds and seals one request payload. Call it again for every attempt: nonces are never reused. */
internal class RequestBuilder(private val crypto: WireCrypto, private val clock: () -> Long) {

    fun http(url: String, method: String, headers: Map<String, String>, body: JsonElement?, timeoutSeconds: Int?): String {
        require(body == null || body is JsonObject || (body is JsonPrimitive && body.isString)) {
            "body must be a JsonObject or a JSON string"
        }
        return seal(httpFields(url, method, headers, body, timeoutSeconds))
    }

    private fun httpFields(
        url: String,
        method: String,
        headers: Map<String, String>,
        body: JsonElement?,
        timeoutSeconds: Int?,
    ): Map<String, JsonElement> = buildMap {
        put("url", JsonPrimitive(url))
        put("method", JsonPrimitive(method.uppercase()))
        if (headers.isNotEmpty()) put("headers", JsonObject(headers.mapValues { JsonPrimitive(it.value) }))
        if (body != null) put("body", body)
        if (timeoutSeconds != null) put("timeout", JsonPrimitive(timeoutSeconds))
    }

    /** A GET with `"raw": true`: the gateway answers with the exact bytes in `body_b64`. */
    fun download(url: String, headers: Map<String, String>, timeoutSeconds: Int?): String =
        seal(httpFields(url, "GET", headers, null, timeoutSeconds) + ("raw" to JsonPrimitive(true)))

    fun publish(topic: String, message: String, qos: Int, retain: Boolean, broker: Broker?): String {
        requireQos(qos)
        return seal(buildMap {
            put("topic", JsonPrimitive(topic))
            put("message", JsonPrimitive(message))
            put("qos", JsonPrimitive(qos))
            put("retain", JsonPrimitive(retain))
            putBroker(broker)
        })
    }

    fun subscribe(topics: List<String>, qos: Int, broker: Broker?): String {
        requireQos(qos)
        return seal(buildMap {
            put("topics", JsonArray(topics.map { JsonPrimitive(it) }))
            put("qos", JsonPrimitive(qos))
            putBroker(broker)
        })
    }

    private fun requireQos(qos: Int) = require(qos in 0..2) { "qos must be 0, 1 or 2" }

    private fun MutableMap<String, JsonElement>.putBroker(broker: Broker?) {
        if (broker == null) return
        broker.host?.let { put("broker_host", JsonPrimitive(it)) }
        broker.port?.let { put("broker_port", JsonPrimitive(it)) }
        broker.username?.let { put("username", JsonPrimitive(it)) }
        broker.password?.let { put("password", JsonPrimitive(it)) }
    }

    private fun seal(fields: Map<String, JsonElement>): String =
        crypto.seal(JsonObject(fields + ("timestamp" to JsonPrimitive(clock()))))
}
