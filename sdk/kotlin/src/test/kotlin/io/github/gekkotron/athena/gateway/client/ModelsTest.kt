package io.github.gekkotron.athena.gateway.client

import kotlinx.serialization.Serializable
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

class ModelsTest {
    @Serializable data class State(val on: Boolean)

    @Test fun `bodyAs decodes and ignores unknown keys`() {
        val r = GatewayResponse(200, buildJsonObject { put("on", true); put("extra", 1) })
        assertEquals(State(true), r.bodyAs<State>())
    }

    @Test fun `backoff grows by factor and caps at max`() {
        val b = Backoff(initial = 1.seconds, max = 5.seconds, factor = 2.0)
        assertEquals(listOf(1, 2, 4, 5, 5).map { it.seconds }, (0..4).map(b::delayFor))
    }

    @Test fun `backoff validates its arguments`() {
        assertFailsWith<IllegalArgumentException> { Backoff(initial = 0.milliseconds) }
        assertFailsWith<IllegalArgumentException> { Backoff(initial = 2.seconds, max = 1.seconds) }
        assertFailsWith<IllegalArgumentException> { Backoff(factor = 0.5) }
    }

    @Test fun `only stream and transport failures are retryable`() {
        assertTrue(GatewayException.StreamError("x").retryable)
        assertTrue(GatewayException.Transport(java.io.IOException("x")).retryable)
        assertFalse(GatewayException.Rejected("x").retryable)
        assertFalse(GatewayException.Unauthorized().retryable)
        assertFalse(GatewayException.GatewayError("x").retryable)
    }
}
