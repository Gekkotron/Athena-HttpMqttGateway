package io.github.gekkotron.athena.gateway.client

import io.github.gekkotron.athena.gateway.client.internal.Frame
import io.github.gekkotron.athena.gateway.client.internal.RawResponse
import io.github.gekkotron.athena.gateway.client.internal.ResponseDecoder
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.add
import kotlinx.serialization.json.buildJsonArray
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs

internal fun envelope(status: Int, body: JsonElement, source: String? = null): RawResponse =
    RawResponse(200, "application/octet-stream", sealed {
        put("status", status); put("body", body); put("timestamp", 0)
        if (source != null) put("source", source)
    })

class ResponseDecoderTest {
    private val decoder = ResponseDecoder(testCrypto)
    private val json = buildJsonObject { put("on", true) }

    @Test fun `plaintext 401 and 400`() {
        assertFailsWith<GatewayException.Unauthorized> { decoder.http(RawResponse(401, null, ""), true) }
        assertFailsWith<GatewayException.BadRequest> { decoder.http(RawResponse(400, null, "{}"), true) }
    }

    @Test fun `other plaintext status is a gateway error`() {
        val e = assertFailsWith<GatewayException.GatewayError> { decoder.http(RawResponse(502, null, "bad gateway"), true) }
        assertEquals("HTTP 502", e.reason)
    }

    @Test fun `undecryptable 200 is a gateway error`() {
        val e = assertFailsWith<GatewayException.GatewayError> { decoder.http(RawResponse(200, null, "Zm9vYmFyYmF6cXV4cXV1eHh4eHh4eHh4eHh4eHh4"), true) }
        assertEquals("Response could not be decrypted", e.reason)
    }

    @Test fun `http success returns status and body as-is`() {
        assertEquals(GatewayResponse(200, json), decoder.http(envelope(200, json), true))
        assertEquals(GatewayResponse(200, JsonPrimitive("plain text")), decoder.http(envelope(200, JsonPrimitive("plain text")), true))
        val array = buildJsonArray { add(1) }
        assertEquals(GatewayResponse(200, array), decoder.http(envelope(200, array), true))
    }

    @Test fun `upstream failure throws or is returned`() {
        val e = assertFailsWith<GatewayException.Upstream> { decoder.http(envelope(404, JsonPrimitive("nope")), true) }
        assertEquals(404, e.status)
        assertEquals(GatewayResponse(404, JsonPrimitive("nope")), decoder.http(envelope(404, JsonPrimitive("nope")), false))
    }

    @Test fun `gateway 403 with string body keeps the exact reason`() {
        // /gateway encodes its error body as a JSON string; a skewed phone clock produces this.
        val raw = envelope(403, JsonPrimitive("""{"error": "Request expired"}"""), source = "gateway")
        assertEquals("Request expired", assertFailsWith<GatewayException.Rejected> { decoder.http(raw, false) }.reason)
    }

    @Test fun `gateway 403 with object body and gateway 500`() {
        val rejected = envelope(403, buildJsonObject { put("error", "port not allowed for this secret") }, "gateway")
        assertEquals("port not allowed for this secret", assertFailsWith<GatewayException.Rejected> { decoder.publish(rejected) }.reason)
        val crashed = envelope(500, JsonPrimitive("""{"error": "boom"}"""), "gateway")
        assertEquals("boom", assertFailsWith<GatewayException.GatewayError> { decoder.http(crashed, false) }.reason)
    }

    @Test fun `publish success and broker failure`() {
        val ok = envelope(200, JsonPrimitive("""{"success": true, "topic": "t", "message": "Published successfully"}"""))
        assertEquals(PublishResult("t"), decoder.publish(ok))
        val failed = envelope(500, JsonPrimitive("""{"success": false, "topic": "t", "message": "Failed with code 4"}"""))
        val e = assertFailsWith<GatewayException.Upstream> { decoder.publish(failed) }
        assertEquals(500, e.status)
        assertEquals(JsonPrimitive("Failed with code 4"), (e.body as kotlinx.serialization.json.JsonObject)["message"])
    }

    @Test fun `stream rejection maps gateway errors and never returns`() {
        assertFailsWith<GatewayException.Rejected> {
            decoder.streamRejection(envelope(403, buildJsonObject { put("error", "Request replayed") }, "gateway"))
        }
        assertFailsWith<GatewayException.Unauthorized> { decoder.streamRejection(RawResponse(401, null, "")) }
        assertFailsWith<GatewayException.GatewayError> { decoder.streamRejection(envelope(200, json)) }
    }

    @Test fun `frames decode into events`() {
        val connected = decoder.frame(sealed { put("type", "connected"); put("topic", "a"); put("topics", buildJsonArray { add("a"); add("b") }) })
        assertEquals(Frame.Event(MqttEvent.Connected(listOf("a", "b"))), connected)

        val legacy = decoder.frame(sealed { put("type", "connected"); put("topic", "a") })
        assertEquals(Frame.Event(MqttEvent.Connected(listOf("a"))), legacy)

        val msg = decoder.frame(sealed {
            put("type", "message"); put("topic", "a"); put("payload", "22.5 C"); put("qos", 1); put("retain", true); put("timestamp", 7)
        })
        assertEquals(Frame.Event(MqttEvent.Message("a", JsonPrimitive("22.5 C"), 1, true, 7)), msg)

        assertEquals(Frame.Error("Connection failed with code 5"), decoder.frame(sealed { put("type", "error"); put("message", "Connection failed with code 5") }))
        assertEquals(Frame.Disconnected, decoder.frame(sealed { put("type", "disconnected"); put("message", "bye") }))
        assertEquals(Frame.Ignored, decoder.frame(sealed { put("type", "future-type") }))
    }

    @Test fun `undecryptable frame is a gateway error`() {
        assertIs<GatewayException.GatewayError>(runCatching { decoder.frame("AAAA") }.exceptionOrNull())
    }
}
