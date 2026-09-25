package io.github.gekkotron.athena.gateway.client

import kotlinx.coroutines.test.runTest
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class AthenaGatewayClientTest {
    private val transport = FakeTransport()
    private val client = AthenaGatewayClient(transport, testCrypto) { 42 }

    @Test fun `public constructor validates key and url eagerly`() {
        assertFailsWith<IllegalArgumentException> { AthenaGatewayClient("https://h.example", "abc") }
        assertFailsWith<IllegalArgumentException> { AthenaGatewayClient("nope", TEST_KEY) }
        AthenaGatewayClient("https://h.example", TEST_KEY)
    }

    @Test fun `http posts a sealed request to gateway and decodes the reply`() = runTest {
        val state = buildJsonObject { put("on", true) }
        transport.postReply = { envelope(200, state) }
        assertEquals(GatewayResponse(200, state), client.http("http://10.0.0.1/state"))
        val (path, body) = transport.posts.single()
        assertEquals("gateway", path)
        assertEquals(
            buildJsonObject { put("url", "http://10.0.0.1/state"); put("method", "GET"); put("timestamp", 42) },
            opened(body),
        )
    }

    @Test fun `http upstream error can be returned instead of thrown`() = runTest {
        transport.postReply = { envelope(404, JsonPrimitive("nope")) }
        assertFailsWith<GatewayException.Upstream> { client.http("http://h") }
        assertEquals(404, client.http("http://h", throwOnUpstreamError = false).status)
    }

    @Test fun `publish posts to mqtt publish`() = runTest {
        transport.postReply = { envelope(200, JsonPrimitive("""{"success": true, "topic": "t", "message": "ok"}""")) }
        assertEquals(PublishResult("t"), client.publish("t", "on", qos = 1))
        assertEquals("mqtt/publish", transport.posts.single().first)
        assertEquals(JsonPrimitive(1), opened(transport.posts.single().second)["qos"])
    }

    @Test fun `every call is freshly encrypted`() = runTest {
        transport.postReply = { envelope(200, JsonPrimitive("""{"success": true, "topic": "t", "message": "ok"}""")) }
        client.publish("t", "m"); client.publish("t", "m")
        assertEquals(2, transport.posts.map { it.second }.toSet().size)
    }

    @Test fun `http read timeout is timeoutSeconds plus 15, defaulting to 45`() = runTest {
        transport.postReply = { envelope(200, JsonPrimitive("ok")) }
        client.http("http://h", timeoutSeconds = 60)
        assertEquals(75, transport.posts.last().third)
        client.http("http://h")
        assertEquals(45, transport.posts.last().third)
    }

    @Test fun `http and publish decode on the given dispatcher`() = runTest {
        val recording = RecordingDispatcher()
        val decodingClient = AthenaGatewayClient(transport, testCrypto, decodeDispatcher = recording) { 42 }
        transport.postReply = { envelope(200, JsonPrimitive("""{"success": true, "topic": "t", "message": "ok"}""")) }

        decodingClient.publish("t", "m")
        assertTrue(recording.dispatches > 0)

        val afterPublish = recording.dispatches
        decodingClient.http("http://h")
        assertTrue(recording.dispatches > afterPublish)
    }
}
