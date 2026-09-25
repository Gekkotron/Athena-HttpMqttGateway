package io.github.gekkotron.athena.gateway.client

import io.github.gekkotron.athena.gateway.client.internal.RawResponse
import io.github.gekkotron.athena.gateway.client.internal.StreamItem
import java.io.IOException
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.advanceTimeBy
import kotlinx.coroutines.test.currentTime
import kotlinx.coroutines.test.runTest
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.add
import kotlinx.serialization.json.buildJsonArray
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.time.Duration.Companion.seconds

class SubscribeTest {
    private val transport = FakeTransport()
    private val client = AthenaGatewayClient(transport, testCrypto) { 42 }

    private fun connected(vararg topics: String) = StreamItem.Data(sealed {
        put("type", "connected"); put("topics", buildJsonArray { topics.forEach { add(it) } })
    })
    private fun message(topic: String, payload: String) = StreamItem.Data(sealed {
        put("type", "message"); put("topic", topic); put("payload", payload); put("qos", 0); put("retain", false); put("timestamp", 1)
    })
    private val disconnected = StreamItem.Data(sealed { put("type", "disconnected"); put("message", "bye") })
    private val errorFrame = StreamItem.Data(sealed { put("type", "error"); put("message", "Connection failed with code 5") })

    @Test fun `streams events until disconnected`() = runTest {
        transport.streamReplies += flowOf(connected("a", "b"), message("a", "on"), disconnected, message("a", "never"))
        val events = client.subscribe("a", "b").toList()
        assertEquals(
            listOf(MqttEvent.Connected(listOf("a", "b")), MqttEvent.Message("a", JsonPrimitive("on"), 0, false, 1)),
            events,
        )
        val (path, body) = transport.streams.single()
        assertEquals("mqtt/subscribe", path)
        assertEquals(buildJsonObject {
            put("topics", buildJsonArray { add("a"); add("b") }); put("qos", 0); put("timestamp", 42)
        }, opened(body))
    }

    @Test fun `error frame fails the flow without reconnect`() = runTest {
        transport.streamReplies += flowOf(connected("a"), errorFrame)
        val e = assertFailsWith<GatewayException.StreamError> { client.subscribe("a").toList() }
        assertEquals("Connection failed with code 5", e.reason)
    }

    @Test fun `pre-stream rejection is surfaced`() = runTest {
        transport.streamReplies += flowOf(
            StreamItem.NotAStream(envelope(403, buildJsonObject { put("error", "port not allowed for this secret") }, "gateway")),
        )
        assertFailsWith<GatewayException.Rejected> { client.subscribe("a").toList() }
    }

    @Test fun `reconnects with backoff and a fresh request each time`() = runTest {
        transport.streamReplies += flowOf(connected("a"), disconnected)                   // ends -> wait 1 s
        transport.streamReplies += flow { throw GatewayException.Transport(IOException("reset")) } // -> wait 2 s
        transport.streamReplies += flowOf(connected("a"), message("a", "on"))
        val events = client.subscribe("a", reconnect = Backoff(initial = 1.seconds, max = 10.seconds)).take(3).toList()

        assertEquals(3, events.size)
        assertEquals(3_000, currentTime)
        assertEquals(3, transport.streams.map { it.second }.toSet().size) // three distinct ciphertexts
    }

    @Test fun `backoff resets after a successful connect`() = runTest {
        repeat(3) { transport.streamReplies += flowOf(connected("a"), disconnected) }
        client.subscribe("a", reconnect = Backoff(initial = 1.seconds, max = 10.seconds)).take(3).toList()
        assertEquals(2_000, currentTime) // 1 s + 1 s, not 1 s + 2 s
    }

    @Test fun `a plaintext 503 (gateway restarting behind a proxy) is retried`() = runTest {
        transport.streamReplies += flowOf(StreamItem.NotAStream(RawResponse(503, null, "")))
        transport.streamReplies += flowOf(connected("a"))
        val events = client.subscribe("a", reconnect = Backoff(initial = 1.seconds)).take(1).toList()
        assertEquals(listOf(MqttEvent.Connected(listOf("a"))), events)
        assertEquals(2, transport.streams.size)
    }

    @Test fun `rejection is never retried`() = runTest {
        transport.streamReplies += flowOf(
            StreamItem.NotAStream(envelope(403, buildJsonObject { put("error", "Request expired") }, "gateway")),
        )
        val e = assertFailsWith<GatewayException.Rejected> {
            client.subscribe("a", reconnect = Backoff()).toList()
        }
        assertEquals("Request expired", e.reason)
        assertEquals(1, transport.streams.size)
    }

    @Test fun `cancelling during backoff sends no further request`() = runTest {
        transport.streamReplies += flow { throw GatewayException.Transport(IOException("down")) }
        val job = launch { client.subscribe("a", reconnect = Backoff(initial = 5.seconds)).collect {} }
        advanceTimeBy(1_000)
        job.cancel()
        advanceTimeBy(60_000)
        assertEquals(1, transport.streams.size)
    }

    @Test fun `at least one topic is required`() {
        assertFailsWith<IllegalArgumentException> { client.subscribe() }
    }
}
