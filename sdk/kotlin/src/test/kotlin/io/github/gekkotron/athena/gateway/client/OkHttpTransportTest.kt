package io.github.gekkotron.athena.gateway.client

import io.github.gekkotron.athena.gateway.client.internal.OkHttpTransport
import io.github.gekkotron.athena.gateway.client.internal.RawResponse
import io.github.gekkotron.athena.gateway.client.internal.StreamItem
import java.util.concurrent.TimeUnit
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import okhttp3.OkHttpClient
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import okhttp3.mockwebserver.SocketPolicy
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class OkHttpTransportTest {
    private val server = MockWebServer()
    private lateinit var transport: OkHttpTransport

    @BeforeEach fun start() {
        server.start()
        transport = OkHttpTransport(server.url("/").toString(), OkHttpClient())
    }

    @AfterEach fun stop() = server.shutdown()

    @Test fun `urls work with and without trailing slash and with a prefix`() {
        assertEquals("https://h.example/gateway", OkHttpTransport("https://h.example", OkHttpClient()).urlFor("gateway").toString())
        assertEquals("https://h.example/mqtt/publish", OkHttpTransport("https://h.example/", OkHttpClient()).urlFor("mqtt/publish").toString())
        assertEquals("https://h.example/athena/mqtt/subscribe", OkHttpTransport("https://h.example/athena", OkHttpClient()).urlFor("mqtt/subscribe").toString())
        assertEquals("https://h.example/athena/gateway", OkHttpTransport("https://h.example/athena/", OkHttpClient()).urlFor("gateway").toString())
    }

    @Test fun `invalid base url is rejected at construction`() {
        assertFailsWith<IllegalArgumentException> { OkHttpTransport("not a url", OkHttpClient()) }
    }

    @Test fun `post sends the body as octet-stream`() = runBlocking {
        server.enqueue(MockResponse().setHeader("Content-Type", "application/octet-stream").setBody("ENC"))
        assertEquals(RawResponse(200, "application/octet-stream", "ENC"), transport.post("mqtt/publish", "BODY"))
        val req = server.takeRequest()
        assertEquals("POST", req.method)
        assertEquals("/mqtt/publish", req.path)
        assertEquals("BODY", req.body.readUtf8())
        assertTrue(req.getHeader("Content-Type")!!.startsWith("application/octet-stream"))
    }

    @Test fun `post io failure is a Transport exception`() = runBlocking {
        server.enqueue(MockResponse().setSocketPolicy(SocketPolicy.DISCONNECT_AT_START))
        assertFailsWith<GatewayException.Transport> { transport.post("gateway", "B") }
        Unit
    }

    @Test fun `stream yields data lines and skips keepalives`() = runBlocking {
        server.enqueue(MockResponse().setHeader("Content-Type", "text/event-stream").setBody(": keepalive\n\ndata: AAA\n\ndata: BBB\n\n"))
        val items = withTimeout(5_000) { transport.stream("mqtt/subscribe", "B").toList() }
        assertEquals(listOf(StreamItem.Data("AAA"), StreamItem.Data("BBB")), items)
        assertEquals("text/event-stream", server.takeRequest().getHeader("Accept"))
    }

    @Test fun `non-stream response is handed back whole`() = runBlocking {
        server.enqueue(MockResponse().setHeader("Content-Type", "application/octet-stream").setBody("ENC"))
        val items = withTimeout(5_000) { transport.stream("mqtt/subscribe", "B").toList() }
        assertEquals(listOf(StreamItem.NotAStream(RawResponse(200, "application/octet-stream", "ENC"))), items)
    }

    @Test fun `stream io failure is a Transport exception`() = runBlocking {
        server.enqueue(MockResponse().setSocketPolicy(SocketPolicy.DISCONNECT_AT_START))
        assertFailsWith<GatewayException.Transport> { withTimeout(5_000) { transport.stream("mqtt/subscribe", "B").toList() } }
        Unit
    }

    @Test fun `cancelling the collector closes an open stream`() = runBlocking {
        // 9 bytes = "data: A\n\n", then the server stalls 4 s; a working cancel returns well within 2 s.
        server.enqueue(
            MockResponse().setHeader("Content-Type", "text/event-stream")
                .setBody("data: A\n\ndata: B\n\n").throttleBody(9, 4, TimeUnit.SECONDS),
        )
        val first = withTimeout(2_000) { transport.stream("mqtt/subscribe", "B").first() }
        assertEquals(StreamItem.Data("A"), first)
    }

    @Test fun `stream read timeout outlasts the 15 s server keepalive`() {
        assertEquals(45_000, transport.streamClient.readTimeoutMillis)
    }
}
