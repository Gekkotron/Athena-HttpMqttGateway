package io.github.gekkotron.athena.gateway.client.internal

import io.github.gekkotron.athena.gateway.client.GatewayException
import java.io.IOException
import java.util.concurrent.TimeUnit
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.channels.awaitClose
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.callbackFlow
import kotlinx.coroutines.launch
import kotlinx.coroutines.suspendCancellableCoroutine
import okhttp3.Call
import okhttp3.Callback
import okhttp3.HttpUrl
import okhttp3.HttpUrl.Companion.toHttpUrl
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import okhttp3.Response

internal sealed interface StreamItem {
    /** One SSE `data:` payload (an encrypted frame). */
    data class Data(val data: String) : StreamItem

    /** The server answered without an event stream (a rejection); body read whole. */
    data class NotAStream(val response: RawResponse) : StreamItem
}

internal interface Transport {
    /**
     * @param readTimeoutSeconds when set, raises the read timeout for this call alone (never
     *   lowers it below the client's own configured read timeout).
     * @throws GatewayException.Transport on I/O failure.
     */
    suspend fun post(path: String, body: String, readTimeoutSeconds: Int? = null): RawResponse

    /** Cold flow; each collection opens one connection. Ends when the server closes it. */
    fun stream(path: String, body: String): Flow<StreamItem>
}

internal class OkHttpTransport(baseUrl: String, private val client: OkHttpClient) : Transport {
    private val base: HttpUrl = baseUrl.toHttpUrl()

    /**
     * Streams sit idle between messages; the server sends a keepalive every 15 s. `callTimeout`
     * is disabled: a shared client's overall call timeout would otherwise cut off every stream.
     */
    internal val streamClient: OkHttpClient = client.newBuilder()
        .readTimeout(45, TimeUnit.SECONDS)
        .callTimeout(0, TimeUnit.SECONDS)
        .build()

    internal fun urlFor(path: String): HttpUrl = base.newBuilder().addPathSegments(path).build()

    override suspend fun post(path: String, body: String, readTimeoutSeconds: Int?): RawResponse =
        suspendCancellableCoroutine { cont ->
            val callClient = if (readTimeoutSeconds == null) {
                client
            } else {
                val millis = maxOf(readTimeoutSeconds * 1000L, client.readTimeoutMillis.toLong())
                client.newBuilder().readTimeout(millis, TimeUnit.MILLISECONDS).build()
            }
            val call = callClient.newCall(request(path, body))
            cont.invokeOnCancellation { call.cancel() }
            call.enqueue(object : Callback {
                override fun onFailure(call: Call, e: IOException) {
                    cont.resumeWithException(GatewayException.Transport(e))
                }

                override fun onResponse(call: Call, response: Response) {
                    try {
                        cont.resume(response.use { it.toRaw() })
                    } catch (e: IOException) {
                        cont.resumeWithException(GatewayException.Transport(e))
                    }
                }
            })
        }

    override fun stream(path: String, body: String): Flow<StreamItem> = callbackFlow {
        val call = streamClient.newCall(
            request(path, body).newBuilder().header("Accept", "text/event-stream").build(),
        )
        launch(Dispatchers.IO) {
            try {
                call.execute().use { response ->
                    val type = response.body?.contentType()
                    if (type?.type != "text" || type.subtype != "event-stream") {
                        send(StreamItem.NotAStream(response.toRaw()))
                    } else {
                        val source = response.body!!.source()
                        while (true) {
                            val line = source.readUtf8Line() ?: break
                            if (line.startsWith("data:")) send(StreamItem.Data(line.substring(5).trim()))
                        }
                    }
                }
                channel.close()
            } catch (e: IOException) {
                channel.close(if (call.isCanceled()) null else GatewayException.Transport(e))
            }
        }
        awaitClose { call.cancel() }
    }

    private fun request(path: String, body: String): Request =
        Request.Builder().url(urlFor(path)).post(body.encodeToByteArray().toRequestBody(OCTET_STREAM)).build()

    private fun Response.toRaw(): RawResponse =
        RawResponse(code, body?.contentType()?.toString(), body?.string().orEmpty())

    private companion object {
        val OCTET_STREAM = "application/octet-stream".toMediaType()
    }
}
