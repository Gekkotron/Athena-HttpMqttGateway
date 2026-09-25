package io.github.gekkotron.athena.gateway.client

import io.github.gekkotron.athena.gateway.client.internal.RequestBuilder
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.add
import kotlinx.serialization.json.buildJsonArray
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import kotlinx.serialization.json.putJsonObject
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotEquals

class RequestBuilderTest {
    private val builder = RequestBuilder(testCrypto) { 1_700_000_000 }

    @Test fun `http payload with defaults omits optional fields`() {
        assertEquals(
            buildJsonObject { put("url", "http://10.0.0.1/x"); put("method", "GET"); put("timestamp", 1_700_000_000) },
            opened(builder.http("http://10.0.0.1/x", "get", emptyMap(), null, null)),
        )
    }

    @Test fun `http payload with headers, object body and timeout`() {
        val body = buildJsonObject { put("on", true) }
        assertEquals(
            buildJsonObject {
                put("url", "http://h"); put("method", "POST")
                putJsonObject("headers") { put("X-A", "1") }
                put("body", body); put("timeout", 5); put("timestamp", 1_700_000_000)
            },
            opened(builder.http("http://h", "POST", mapOf("X-A" to "1"), body, 5)),
        )
    }

    @Test fun `http accepts a raw string body`() {
        assertEquals(JsonPrimitive("raw"), opened(builder.http("http://h", "POST", emptyMap(), JsonPrimitive("raw"), null))["body"])
    }

    @Test fun `http rejects array and number bodies`() {
        assertFailsWith<IllegalArgumentException> { builder.http("http://h", "POST", emptyMap(), buildJsonArray { add(1) }, null) }
        assertFailsWith<IllegalArgumentException> { builder.http("http://h", "POST", emptyMap(), JsonPrimitive(1), null) }
    }

    @Test fun `publish payload with broker overrides`() {
        assertEquals(
            buildJsonObject {
                put("topic", "t"); put("message", "m"); put("qos", 1); put("retain", true)
                put("broker_host", "10.0.0.9"); put("broker_port", 1884); put("username", "u"); put("password", "p")
                put("timestamp", 1_700_000_000)
            },
            opened(builder.publish("t", "m", 1, true, Broker("10.0.0.9", 1884, "u", "p"))),
        )
    }

    @Test fun `subscribe payload sends topics list`() {
        assertEquals(
            buildJsonObject {
                put("topics", buildJsonArray { add("a/#"); add("b/+") }); put("qos", 0); put("timestamp", 1_700_000_000)
            },
            opened(builder.subscribe(listOf("a/#", "b/+"), 0, null)),
        )
    }

    @Test fun `qos outside 0 to 2 is rejected`() {
        assertFailsWith<IllegalArgumentException> { builder.publish("t", "m", 3, false, null) }
        assertFailsWith<IllegalArgumentException> { builder.subscribe(listOf("t"), -1, null) }
    }

    @Test fun `every call produces a new ciphertext`() {
        assertNotEquals(builder.subscribe(listOf("t"), 0, null), builder.subscribe(listOf("t"), 0, null))
    }
}
