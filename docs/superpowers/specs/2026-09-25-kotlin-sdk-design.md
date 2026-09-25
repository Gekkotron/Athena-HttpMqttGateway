# Athena Gateway Client (Kotlin SDK) — Design

- **Date:** 2026-09-25
- **Author:** Gekkotron
- **Status:** Approved in conversation; awaiting spec review

## 1. Goal

A public Kotlin library that lets an app (first consumer: the upcoming public
**Oikos** Android app) talk to the gateway in a few lines: forward HTTP
requests, publish to MQTT, and stream MQTT topics live — without the app
knowing the wire format, the crypto, or the replay rules.

**Success criteria**

1. An Android app adds one repository line + one dependency and can call all
   three endpoints.
2. Every request the SDK sends is accepted by the gateway at the same commit
   (proved in CI against the real server, not a mock).
3. Gateway errors surface as typed Kotlin exceptions, never as raw bytes.
4. Nonce/timestamp handling is impossible to get wrong from the app side.

## 2. Decisions

| Topic | Decision |
|---|---|
| Platform | Pure Kotlin/JVM library (no Android framework dependency), Java 11 bytecode |
| Location | `sdk/kotlin/` in this repo |
| Naming | Artifact `athena-gateway-client`, entry class `AthenaGatewayClient` — "client" makes clear the library talks to the gateway, it is not the gateway |
| Distribution | Public GitHub repo, built by **JitPack** from git tags; no Maven Central |
| Versioning | Server and SDK share repo tags (`v1.1.0` = first tag) |
| HTTP | OkHttp 4.12. SSE is parsed in-house (no `okhttp-sse`): the SDK must read the body of a non-stream rejection and needs a stream read timeout above the server's 15 s keepalive (OkHttp's default is 10 s) |
| Async | kotlinx-coroutines: `suspend` calls, `Flow` for subscribe |
| JSON | kotlinx-serialization-json |
| Crypto | JDK `javax.crypto` AES/GCM/NoPadding (available on all Android levels) |
| Package | `io.github.gekkotron.athena.gateway.client` |

### Install coordinates

JitPack builds non-root projects via `jitpack.yml`; artifacts published from
a subproject use the multi-module coordinate form:

```kotlin
repositories { maven("https://jitpack.io") }
dependencies {
    implementation("com.github.Gekkotron.Athena-HttpMqttGateway:athena-gateway-client:v1.1.0")
}
```

(Supersedes the `com.github.Gekkotron:Athena-HttpMqttGateway:…` form shown
during brainstorming.) The implementation plan includes a JitPack build of a
pre-release tag to confirm these exact coordinates before `v1.1.0`.

## 3. Wire format (what the SDK must match)

Source of truth: `server/crypto.py`, `server/app.py`, `server/gateway.py`,
`server/replay.py`.

- **Request body:** `base64( nonce[12] ‖ AES-256-GCM(key, nonce, json, aad=none) )`.
  Tag is the GCM default 128 bits, appended to the ciphertext.
- **Key:** 64 hex chars (32 bytes).
- **Every payload** carries `"timestamp": <unix seconds>`; the server rejects
  `|now − timestamp| > MAX_AGE_SECONDS` (default 60) and any nonce already
  seen within that window.
- **Plaintext failures** (no key could be chosen): `401` empty body (no key
  matched), `400` (malformed base64 / ciphertext).
- **Encrypted responses** (HTTP 200 carrying base64 ciphertext) decrypt to
  `{"status": Int, "body": …, "timestamp": Int}`.
  - `/gateway` success: `body` = upstream JSON object/array, or text string.
  - `/mqtt/publish` success/failure: `body` = **JSON-encoded string** of
    `{"success", "topic", "message"}` or `{"error"}`.
  - Gateway-issued errors: `body` = `{"error": "…"}` — a string on
    `/gateway`, an object on `/mqtt/*` (inconsistent today).
- **`/mqtt/subscribe`:** `text/event-stream`; each `data:` line is one
  encrypted frame decrypting to `{"type": "connected"|"message"|"error"|"disconnected", …}`.
  Lines starting with `:` are keepalives. Pre-stream rejections come back as
  a normal encrypted (non-SSE) response.

### Server change (part of this work)

Add `"source": "gateway"` to every gateway-issued encrypted error
(`app._encrypted_error`, `GatewayHandler._encrypted_error_response`,
`MQTTService` 400/500 payloads). Backward-compatible: existing clients ignore
unknown fields. Lets the SDK distinguish a gateway 403 from an upstream
device's 403. Covered by new pytest cases.

## 4. Public API

```kotlin
class AthenaGatewayClient(
    baseUrl: String,
    secretKey: String,                         // 64 hex chars; validated eagerly
    okHttpClient: OkHttpClient = OkHttpClient(),
    clock: () -> Long = { System.currentTimeMillis() / 1000 },  // for tests
) {
    suspend fun http(
        url: String,
        method: String = "GET",
        headers: Map<String, String> = emptyMap(),
        body: JsonElement? = null,             // JsonObject → JSON, JSON string → raw; anything else → IllegalArgumentException
        timeoutSeconds: Int? = null,
        throwOnUpstreamError: Boolean = true,
    ): GatewayResponse

    suspend fun publish(
        topic: String,
        message: String,
        qos: Int = 0,
        retain: Boolean = false,
        broker: Broker? = null,                // host/port/username/password overrides
    ): PublishResult

    fun subscribe(
        vararg topics: String,
        qos: Int = 0,
        broker: Broker? = null,
        reconnect: Backoff? = null,            // null = no auto-reconnect
    ): Flow<MqttEvent>
}

data class GatewayResponse(val status: Int, val body: JsonElement) {
    inline fun <reified T> bodyAs(json: Json = Json { ignoreUnknownKeys = true }): T
}
data class PublishResult(val topic: String)
data class Broker(val host: String? = null, val port: Int? = null,
                  val username: String? = null, val password: String? = null)
data class Backoff(val initial: Duration = 1.seconds, val max: Duration = 60.seconds,
                   val factor: Double = 2.0)

sealed interface MqttEvent {
    data class Connected(val topics: List<String>) : MqttEvent
    data class Message(val topic: String, val payload: JsonElement, val qos: Int,
                       val retain: Boolean, val timestamp: Long) : MqttEvent
}

sealed class GatewayException(message: String, cause: Throwable? = null) : Exception(message, cause) {
    class Unauthorized : GatewayException("No gateway secret matched this key")
    class BadRequest : GatewayException("Gateway could not parse the request")
    class Rejected(val reason: String) : GatewayException(reason)          // gateway 403
    class GatewayError(val reason: String) : GatewayException(reason)      // gateway 5xx
    class Upstream(val status: Int, val body: JsonElement) : GatewayException("Upstream returned $status")
    class StreamError(val reason: String) : GatewayException(reason)       // SSE error frame
    class Transport(cause: java.io.IOException) : GatewayException(cause.message ?: "I/O error", cause)
}
```

`subscribe` requires at least one topic (`IllegalArgumentException`
otherwise). A broker-side publish failure (`success: false`, not tagged
`source: "gateway"`) throws `Upstream(status, body)`; a missing field or
connection error inside the gateway (tagged) throws `GatewayError`.

## 5. Internals

All `internal`; one job per file.

| Unit | Responsibility | Depends on |
|---|---|---|
| `WireCrypto` | `seal(JsonObject): String` / `open(bytes): JsonObject`; fresh 12-byte `SecureRandom` nonce per call | JDK crypto |
| `RequestBuilder` | Builds each endpoint's payload, stamps `timestamp` from `clock`, seals it. Called anew for every attempt. | `WireCrypto` |
| `Transport` / `OkHttpTransport` | `post(path, body): RawResponse`; `stream(path, body): Flow<StreamItem>` (SSE `data` lines, or the non-stream response) via `callbackFlow`, cancelling the OkHttp `Call` in `awaitClose`; stream read timeout 45 s | OkHttp |
| `ResponseDecoder` | Plaintext status → exception; decrypt; normalise `body` (JSON string → parsed element where the endpoint defines it so); map to result or `GatewayException`; decode SSE frames into `MqttEvent` | `WireCrypto` |
| `AthenaGatewayClient` | Public facade wiring the above; reconnect loop for `subscribe` | all |

### Error mapping

1. HTTP 401 → `Unauthorized`; HTTP 400 → `BadRequest`; other non-200
   plaintext → `GatewayError("HTTP <code>")`.
2. Decrypt the 200 body. If `source == "gateway"`: status 403 → `Rejected`,
   anything else → `GatewayError`.
3. Otherwise (`/gateway`): 2xx → `GatewayResponse`; non-2xx →
   `Upstream` if `throwOnUpstreamError`, else returned as `GatewayResponse`.
4. `IOException` anywhere → `Transport`. Decryption failure of a 200 body →
   `GatewayError("Response could not be decrypted")`.

### Subscribe / reconnect

- Each attempt: build a fresh request → POST. If the response is not
  `text/event-stream`, it is a pre-stream rejection: decode it with the
  normal error mapping (e.g. expired / port not allowed → `Rejected`).
  Otherwise read SSE → emit `Connected` /
  `Message`; `error` frame → `StreamError`; `disconnected` → end of attempt.
- `reconnect == null`: an ended attempt completes the flow on `disconnected`
  and throws on `StreamError` / `Transport`.
- `reconnect != null`: on `disconnected`, `StreamError`, or `Transport`,
  wait `min(initial · factor^n, max)` and retry with a **new** request; `n`
  resets after a successful `Connected`. `Unauthorized`, `BadRequest`, and
  `Rejected` are never retried (retrying cannot fix them).
- Cancelling the collector cancels the OkHttp `Call` immediately.

### Android packaging

- No consumer R8/ProGuard rules needed: the SDK declares no `@Serializable`
  classes (it works on `JsonElement`), and kotlinx-serialization ships its
  own rules. Apps using `bodyAs<T>()` keep their own `@Serializable` types as
  usual.
- No `android.*` imports; works on JVM desktop/server too.

## 6. Testing

1. **Unit** (JUnit 5, kotlinx-coroutines-test, OkHttp MockWebServer):
   crypto round-trip and a fixed test vector decryptable by Python; nonce
   uniqueness across calls; every error-mapping row above; body
   normalisation per endpoint; SSE frame parsing incl. keepalives;
   cancellation closes the connection; backoff sequence and reset; no retry
   on `Rejected`.
2. **Wire compatibility** (CI job `sdk-integration`): start the Python
   gateway from the same commit with a known test key, a Mosquitto service
   container, and a tiny local HTTP echo server; run a JUnit suite tagged
   `integration` covering `http` round-trip, `publish` → observed by a
   concurrent `subscribe` on two topics, wrong key → `Unauthorized`,
   destination outside scope → `Rejected`.
3. **Coverage:** Kover; CI fails under 85 %; publishes an "SDK coverage"
   badge to the `badges` branch (`sdk-coverage.json`) like the Python one.

## 7. CI & release

- New workflow job(s) in `.github/workflows/ci.yml`: `sdk-unit` (Gradle
  build + tests + Kover) and `sdk-integration`; badge job extended.
- `jitpack.yml` at repo root: JDK 17, `cd sdk/kotlin && ./gradlew publishToMavenLocal`.
- Release = push a `vX.Y.Z` tag; JitPack builds on first request.

## 8. Documentation

- `sdk/kotlin/README.md`: install, quick start (all three calls), error
  handling, reconnect, certificate pinning via custom `OkHttpClient`,
  Oikos-style Android snippet (ViewModel collecting `subscribe`).
- Root README: replace the hand-written Kotlin client section with a short
  pointer to the SDK README; add the SDK coverage badge.

## 9. Out of scope

Android Keystore helpers; Java-friendly (non-suspend) API; automatic retries
for `http` / `publish` (resending an encrypted request is exactly what the
replay cache blocks — callers re-invoke instead); Swift/iOS SDK; Maven
Central publishing.
