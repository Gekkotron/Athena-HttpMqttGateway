package io.github.gekkotron.athena.gateway.client

import io.github.gekkotron.athena.gateway.client.internal.RawResponse
import io.github.gekkotron.athena.gateway.client.internal.StreamItem
import io.github.gekkotron.athena.gateway.client.internal.Transport
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.emptyFlow

internal class FakeTransport : Transport {
    val posts = mutableListOf<Triple<String, String, Int?>>()
    val streams = mutableListOf<Pair<String, String>>()
    var postReply: (path: String) -> RawResponse = { error("no post reply configured") }
    val streamReplies = ArrayDeque<Flow<StreamItem>>()

    override suspend fun post(path: String, body: String, readTimeoutSeconds: Int?): RawResponse {
        posts += Triple(path, body, readTimeoutSeconds)
        return postReply(path)
    }

    override fun stream(path: String, body: String): Flow<StreamItem> {
        streams += path to body
        return streamReplies.removeFirstOrNull() ?: emptyFlow()
    }
}
