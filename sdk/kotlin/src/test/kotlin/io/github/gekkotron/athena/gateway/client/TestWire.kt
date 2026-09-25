package io.github.gekkotron.athena.gateway.client

import io.github.gekkotron.athena.gateway.client.internal.WireCrypto
import kotlin.coroutines.CoroutineContext
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonObjectBuilder
import kotlinx.serialization.json.buildJsonObject

internal val TEST_KEY = "11".repeat(32)
internal val testCrypto = WireCrypto.fromHex(TEST_KEY)

internal fun sealed(block: JsonObjectBuilder.() -> Unit): String = testCrypto.seal(buildJsonObject(block))
internal fun opened(data: String): JsonObject = testCrypto.open(data)

/** Records every dispatch so tests can prove decode work ran on a given dispatcher. */
internal class RecordingDispatcher : CoroutineDispatcher() {
    var dispatches = 0
        private set

    override fun dispatch(context: CoroutineContext, block: Runnable) {
        dispatches++
        Dispatchers.Default.dispatch(context, block)
    }
}
