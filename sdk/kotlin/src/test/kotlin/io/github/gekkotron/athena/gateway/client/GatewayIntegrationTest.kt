package io.github.gekkotron.athena.gateway.client

import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.filterIsInstance
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

/** Runs against a live gateway + broker; see the sdk-integration CI job. */
@Tag("integration")
class GatewayIntegrationTest {
    private fun env(name: String): String = System.getenv(name).orEmpty()

    private lateinit var full: AthenaGatewayClient
    private lateinit var scoped: AthenaGatewayClient

    @BeforeEach fun setUp() {
        assumeTrue(env("GATEWAY_URL").isNotEmpty(), "GATEWAY_URL not set")
        full = AthenaGatewayClient(env("GATEWAY_URL"), env("FULL_KEY"))
        scoped = AthenaGatewayClient(env("GATEWAY_URL"), env("SCOPED_KEY"))
    }

    @Test fun `http round trip through the real gateway`() = runBlocking {
        assertEquals(GatewayResponse(200, buildJsonObject { put("ok", true) }), full.http(env("ECHO_URL")))
    }

    @Test fun `publish is received by a concurrent multi-topic subscribe`() = runBlocking {
        withTimeout(20_000) {
            val connected = kotlinx.coroutines.CompletableDeferred<Unit>()
            val received = async {
                full.subscribe("sdk-it/a", "sdk-it/b")
                    .onEach { if (it is MqttEvent.Connected) connected.complete(Unit) }
                    .filterIsInstance<MqttEvent.Message>()
                    .first()
            }
            connected.await()
            delay(500) // the server emits `connected` before SUBACK; give the broker time to finish subscribing
            assertEquals(PublishResult("sdk-it/b"), full.publish("sdk-it/b", """{"n": 1}"""))
            val msg = received.await()
            assertEquals("sdk-it/b", msg.topic)
            assertEquals(buildJsonObject { put("n", 1) }, msg.payload)
        }
    }

    @Test fun `wrong key is unauthorized`() = runBlocking {
        val stranger = AthenaGatewayClient(env("GATEWAY_URL"), "ab".repeat(32))
        assertFailsWith<GatewayException.Unauthorized> { stranger.http(env("ECHO_URL")) }
        Unit
    }

    @Test fun `scope limits are reported as rejections`() = runBlocking {
        val dest = assertFailsWith<GatewayException.Rejected> { scoped.http("http://10.255.255.1/") }
        assertEquals("destination not allowed for this secret", dest.reason)
        val port = assertFailsWith<GatewayException.Rejected> { scoped.publish("t", "m") }
        assertEquals("port not allowed for this secret", port.reason)
        val stream = assertFailsWith<GatewayException.Rejected> { scoped.subscribe("t").first() }
        assertEquals("port not allowed for this secret", stream.reason)
    }

    @Test fun `text payloads arrive as json strings`() = runBlocking {
        withTimeout(20_000) {
            val connected = kotlinx.coroutines.CompletableDeferred<Unit>()
            val received = async {
                full.subscribe("sdk-it/text")
                    .onEach { if (it is MqttEvent.Connected) connected.complete(Unit) }
                    .filterIsInstance<MqttEvent.Message>()
                    .first()
            }
            connected.await()
            delay(500) // the server emits `connected` before SUBACK; give the broker time to finish subscribing
            full.publish("sdk-it/text", "22.5 C")
            assertEquals(JsonPrimitive("22.5 C"), received.await().payload)
        }
    }
}
