package io.github.gekkotron.athena.gateway.client

import io.github.gekkotron.athena.gateway.client.internal.WireCrypto
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonObjectBuilder
import kotlinx.serialization.json.buildJsonObject

internal val TEST_KEY = "11".repeat(32)
internal val testCrypto = WireCrypto.fromHex(TEST_KEY)

internal fun sealed(block: JsonObjectBuilder.() -> Unit): String = testCrypto.seal(buildJsonObject(block))
internal fun opened(data: String): JsonObject = testCrypto.open(data)
