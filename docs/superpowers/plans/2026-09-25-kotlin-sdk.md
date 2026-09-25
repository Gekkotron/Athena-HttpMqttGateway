# Athena Gateway Client (Kotlin SDK) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship `athena-gateway-client`, a public Kotlin/JVM library in `sdk/kotlin/` that lets an app forward HTTP, publish MQTT, and stream MQTT topics through the gateway in a few lines, installed from JitPack.

**Architecture:** Five internal units with one job each — `WireCrypto` (AES-GCM wire format), `RequestBuilder` (payload + timestamp + seal, fresh per attempt), `Transport`/`OkHttpTransport` (POST + in-house SSE reader), `ResponseDecoder` (decrypt, normalise, map errors) — behind one public facade, `AthenaGatewayClient`. The Python server gains a `"source": "gateway"` tag on its own errors so the SDK can tell them from upstream failures. CI proves wire compatibility by running the SDK against the real server + Mosquitto.

**Tech Stack:** Kotlin 2.0.21 (JVM, Java 11 bytecode), Gradle 8.10.2 wrapper, OkHttp 4.12.0, kotlinx-coroutines 1.9.0, kotlinx-serialization-json 1.7.3, JUnit 5.11.3, MockWebServer 4.12.0, Kover 0.8.3; Python/pytest for the server change; GitHub Actions; JitPack.

**Spec:** `docs/superpowers/specs/2026-09-25-kotlin-sdk-design.md`

## Global Constraints

- Location: `sdk/kotlin/` in this repo. Package: `io.github.gekkotron.athena.gateway.client` (internals in `….client.internal`).
- Maven coordinates: group `com.github.Gekkotron.Athena-HttpMqttGateway`, artifact `athena-gateway-client`; install string `com.github.Gekkotron.Athena-HttpMqttGateway:athena-gateway-client:<tag>`.
- Entry class: `AthenaGatewayClient`. "client" must appear in artifact and class names.
- Pure JVM: no `android.*` imports. Java 11 bytecode. Base64 via okio (bundled with OkHttp) — **not** `java.util.Base64` (Android API 26+ only).
- Kotlin `explicitApi()` mode: every public declaration has an explicit `public` modifier and type.
- Wire format: `base64(nonce[12] ‖ AES-256-GCM(key, nonce, json))`, 128-bit tag, no AAD; every payload has `"timestamp"` (unix seconds).
- Never resend an encrypted request: every attempt (including stream reconnects) builds a new one.
- Stream read timeout 45 s (server keepalive is 15 s).
- Kotlin SDK coverage floor 85 % (Kover); Python floor stays 90 %.
- Attribution: author/byline `Gekkotron`; commits with `git -c user.name=Gekkotron -c user.email=60887050+Gekkotron@users.noreply.github.com commit …` and the trailer `Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>`.
- Do not push tags or create anything on GitHub/JitPack without the user's explicit OK (Task 11 is user-gated).

## Review Focus

1. **Quiet topic for > 10 s** (OkHttp default read timeout) — the stream must stay open; covered by the 45 s stream timeout test in Task 5.
2. **Phone clock off by > 60 s** — the gateway answers "Request expired"; the app must see `Rejected("Request expired")` with that exact reason; test in Task 4.
3. **`baseUrl` with a trailing slash or a path prefix** (`https://h/`, `https://h/athena`) — requests must hit `…/gateway`, `…/athena/mqtt/publish`; test in Task 5.
4. **Collector cancelled while waiting to reconnect** — no further request may be sent; test in Task 7.
5. **Non-JSON MQTT payloads and JSON-array HTTP bodies** — a text payload arrives as a JSON string; an array body fails fast with `IllegalArgumentException` instead of the server forwarding a Python repr; tests in Tasks 3 and 4.

---

## File Structure

```
server/app.py                         (modify) tag gateway errors, /health version 1.1.0
server/gateway.py                     (modify) tag gateway errors
server/services/mqtt_service.py       (modify) tag 400/500 errors
tests/test_app.py, tests/test_services.py  (modify) assert the tag
jitpack.yml                           (create) JitPack build from sdk/kotlin
.github/workflows/ci.yml              (modify) sdk-unit, sdk-integration, badge
README.md                             (modify) SDK pointer + badge
sdk/kotlin/
  settings.gradle.kts, build.gradle.kts, gradle.properties, .gitignore
  gradlew, gradlew.bat, gradle/wrapper/*          (generated)
  README.md
  src/main/kotlin/io/github/gekkotron/athena/gateway/client/
    AthenaGatewayClient.kt   public facade
    Models.kt                GatewayResponse, PublishResult, Broker, Backoff, MqttEvent
    GatewayException.kt      error hierarchy
    internal/WireCrypto.kt   AES-GCM + hex
    internal/RequestBuilder.kt
    internal/ResponseDecoder.kt   RawResponse, Frame, decoding
    internal/Transport.kt    Transport, StreamItem, OkHttpTransport
  src/test/kotlin/io/github/gekkotron/athena/gateway/client/
    TestWire.kt, FakeTransport.kt
    WireCryptoTest.kt, RequestBuilderTest.kt, ModelsTest.kt, ResponseDecoderTest.kt,
    OkHttpTransportTest.kt, AthenaGatewayClientTest.kt, SubscribeTest.kt,
    GatewayIntegrationTest.kt   (@Tag("integration"))
```

Run all Gradle commands from `sdk/kotlin/`. Python commands from the repo root with the project venv (`pip install -r requirements-dev.txt`).

---

### Task 1: Server tags its own errors with `"source": "gateway"`

**Files:**
- Modify: `server/app.py` (`_encrypted_error`, `/health`)
- Modify: `server/gateway.py` (`_encrypted_error_response`)
- Modify: `server/services/mqtt_service.py` (the `KeyError` and `Exception` branches)
- Test: `tests/test_app.py`, `tests/test_services.py`

**Interfaces:**
- Produces: every gateway-issued encrypted error payload contains `"source": "gateway"`. Broker-side publish failures (`rc != 0`) and upstream HTTP responses do **not**. `/health` returns `"version": "1.1.0"`.

- [ ] **Step 1: Write the failing tests** — append to `tests/test_app.py`:

```python
@pytest.mark.parametrize("path, payload", [
    ("/gateway", {"url": "http://10.0.0.1/"}),
    ("/mqtt/publish", {"topic": "t", "message": "m"}),
    ("/mqtt/subscribe", {"topic": "t"}),
])
def test_gateway_errors_are_tagged(client, full, path, payload):
    r = client.post(path, data=full.encrypt({**payload, "timestamp": 0}))  # expired
    assert full.decrypt(r.data)["source"] == "gateway"


def test_upstream_response_is_not_tagged(client, full, now, monkeypatch):
    from server.services import http_service
    resp = mock.Mock(status_code=403, text="denied")
    resp.json.side_effect = ValueError
    monkeypatch.setattr(http_service.requests, "request", mock.Mock(return_value=resp))
    r = client.post("/gateway", data=full.encrypt({"url": "http://10.0.0.1/", "timestamp": now}))
    out = full.decrypt(r.data)
    assert out["status"] == 403 and "source" not in out


def test_health_reports_version(client):
    assert client.get("/health").get_json()["version"] == "1.1.0"
```

Append to `tests/test_services.py`:

```python
def test_mqtt_internal_errors_are_tagged(crypto, secret, fake_mqtt):
    missing = _open(crypto, MQTTService(crypto).handle_request({"topic": "t"}, secret))
    fake_mqtt.connect.side_effect = OSError("refused")
    crashed = _open(crypto, MQTTService(crypto).handle_request({"topic": "t", "message": "m"}, secret))
    assert missing["source"] == crashed["source"] == "gateway"


def test_mqtt_broker_rejection_is_not_tagged(crypto, secret, fake_mqtt):
    fake_mqtt.publish.return_value = SimpleNamespace(rc=4)
    out = _open(crypto, MQTTService(crypto).handle_request({"topic": "t", "message": "m"}, secret))
    assert "source" not in out
```

- [ ] **Step 2: Run to verify they fail**

Run: `python -m pytest tests/test_app.py tests/test_services.py -q -k "tagged or health_reports"`
Expected: FAIL — `KeyError: 'source'` and version `1.0.0 != 1.1.0`.

- [ ] **Step 3: Implement**

`server/app.py` — in `_encrypted_error`, the payload becomes:

```python
    error_payload = {
        "status": status,
        "body": {"error": message},
        "timestamp": int(time.time()),
        "source": "gateway",
    }
```

and `/health` returns `{"status": "ok", "version": "1.1.0"}`.

`server/gateway.py` — in `_encrypted_error_response`:

```python
        error_payload = {
            "status": status,
            "body": json.dumps({"error": message}),
            "timestamp": int(time.time()),
            "source": "gateway",
        }
```

`server/services/mqtt_service.py` — add `"source": "gateway",` to the `response_payload` dict in both the `except KeyError` and the `except Exception` branches (not the success/`rc` branch).

- [ ] **Step 4: Run the whole Python suite**

Run: `python -m pytest -q --cov=server --cov-fail-under=90`
Expected: all pass, coverage ≥ 90 %.

- [ ] **Step 5: Commit**

```bash
git add server tests
git -c user.name=Gekkotron -c user.email=60887050+Gekkotron@users.noreply.github.com commit -m "feat(server): tag gateway-issued errors with source=gateway

Lets clients tell a gateway 403 (expired, replayed, scope) from an
upstream device's 403. Backward-compatible extra field. Bump /health
version to 1.1.0.

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

---

### Task 2: Gradle project + `WireCrypto`

**Files:**
- Create: `sdk/kotlin/settings.gradle.kts`, `sdk/kotlin/build.gradle.kts`, `sdk/kotlin/gradle.properties`, `sdk/kotlin/.gitignore`
- Generate: `sdk/kotlin/gradlew`, `gradlew.bat`, `gradle/wrapper/gradle-wrapper.{jar,properties}`
- Create: `sdk/kotlin/src/main/kotlin/io/github/gekkotron/athena/gateway/client/internal/WireCrypto.kt`
- Test: `sdk/kotlin/src/test/kotlin/io/github/gekkotron/athena/gateway/client/WireCryptoTest.kt`, `TestWire.kt`

**Interfaces:**
- Produces:
  - `internal class WireCrypto(key: ByteArray, random: SecureRandom = SecureRandom())` with `fun seal(payload: JsonObject): String` (base64) and `fun open(data: String): JsonObject` (throws on bad base64 / short input / bad tag / non-object JSON).
  - `WireCrypto.fromHex(hex: String): WireCrypto` — `IllegalArgumentException("secretKey must be 64 hex characters (32 bytes)")` on wrong length/non-hex.
  - Test helpers in `TestWire.kt`: `TEST_KEY`, `testCrypto`, `sealed { … }`.

- [ ] **Step 1: Create the build files**

`sdk/kotlin/settings.gradle.kts`:

```kotlin
rootProject.name = "athena-gateway-client"
```

`sdk/kotlin/gradle.properties`:

```properties
kotlin.code.style=official
org.gradle.caching=true
```

`sdk/kotlin/.gitignore`:

```
.gradle/
.kotlin/
build/
.idea/
local.properties
```

`sdk/kotlin/build.gradle.kts`:

```kotlin
import org.jetbrains.kotlin.gradle.dsl.JvmTarget

plugins {
    kotlin("jvm") version "2.0.21"
    kotlin("plugin.serialization") version "2.0.21"
    id("org.jetbrains.kotlinx.kover") version "0.8.3"
    `java-library`
    `maven-publish`
}

group = "com.github.Gekkotron.Athena-HttpMqttGateway"
if (version == "unspecified") version = "0.0.0-SNAPSHOT"

repositories { mavenCentral() }

java {
    sourceCompatibility = JavaVersion.VERSION_11
    targetCompatibility = JavaVersion.VERSION_11
    withSourcesJar()
}

kotlin {
    explicitApi()
    compilerOptions { jvmTarget.set(JvmTarget.JVM_11) }
}

dependencies {
    api("com.squareup.okhttp3:okhttp:4.12.0")
    api("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.9.0")
    api("org.jetbrains.kotlinx:kotlinx-serialization-json:1.7.3")

    testImplementation(kotlin("test"))
    testImplementation("org.junit.jupiter:junit-jupiter:5.11.3")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    testImplementation("com.squareup.okhttp3:mockwebserver:4.12.0")
    testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:1.9.0")
}

tasks.test {
    useJUnitPlatform { excludeTags("integration") }
}

val integrationTest by tasks.registering(Test::class) {
    description = "Runs tests against a live gateway (see GatewayIntegrationTest)."
    group = "verification"
    testClassesDirs = sourceSets.test.get().output.classesDirs
    classpath = sourceSets.test.get().runtimeClasspath
    useJUnitPlatform { includeTags("integration") }
    outputs.upToDateWhen { false }
}

kover {
    currentProject { instrumentation { disabledForTestTasks.add("integrationTest") } }
    reports { verify { rule { minBound(85) } } }
}
```

- [ ] **Step 2: Generate the Gradle wrapper** (no Gradle is installed locally; use a throwaway distribution in the scratchpad `$S`)

```bash
curl -sSLo "$S/gradle.zip" https://services.gradle.org/distributions/gradle-8.10.2-bin.zip
unzip -q -o "$S/gradle.zip" -d "$S"
cd sdk/kotlin && "$S/gradle-8.10.2/bin/gradle" wrapper --gradle-version 8.10.2 --distribution-type bin
./gradlew --version
```

Expected: `Gradle 8.10.2`. If the `kover { currentProject { instrumentation … } }` block fails to configure, look up the Kover 0.8.3 DSL (context7) for the equivalent "exclude test task from instrumentation" option; keep the intent.

- [ ] **Step 3: Write the failing tests**

`src/test/kotlin/io/github/gekkotron/athena/gateway/client/TestWire.kt`:

```kotlin
package io.github.gekkotron.athena.gateway.client

import io.github.gekkotron.athena.gateway.client.internal.WireCrypto
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonObjectBuilder
import kotlinx.serialization.json.buildJsonObject

internal val TEST_KEY = "11".repeat(32)
internal val testCrypto = WireCrypto.fromHex(TEST_KEY)

internal fun sealed(block: JsonObjectBuilder.() -> Unit): String = testCrypto.seal(buildJsonObject(block))
internal fun opened(data: String): JsonObject = testCrypto.open(data)
```

`src/test/kotlin/io/github/gekkotron/athena/gateway/client/WireCryptoTest.kt`:

```kotlin
package io.github.gekkotron.athena.gateway.client

import io.github.gekkotron.athena.gateway.client.internal.WireCrypto
import java.security.SecureRandom
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import okio.ByteString.Companion.decodeBase64
import okio.ByteString.Companion.toByteString
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotEquals
import kotlin.test.assertTrue

class WireCryptoTest {
    // Produced by the Python server's AESGCM with key 11…11, nonce 00…0b,
    // plaintext {"a": 1, "s": "hé"}.
    private val pythonVector = "AAECAwQFBgcICQoLaMHXMUGDBNg0mqC9/o/VTpXUMFhwTHBSqWbvoS4ZKc5eNqRSVPypxA=="

    private val countingRandom = object : SecureRandom() {
        override fun nextBytes(bytes: ByteArray) { for (i in bytes.indices) bytes[i] = i.toByte() }
    }

    @Test fun `opens a payload sealed by the Python server`() {
        assertEquals(buildJsonObject { put("a", 1); put("s", "hé") }, testCrypto.open(pythonVector))
    }

    @Test fun `seal puts the 12-byte nonce first`() {
        val crypto = WireCrypto(ByteArray(32) { 0x11 }, countingRandom)
        assertTrue(crypto.seal(buildJsonObject { put("a", 1) }).startsWith("AAECAwQFBgcICQoL"))
    }

    @Test fun `round trip and fresh nonce per call`() {
        val payload = buildJsonObject { put("x", "y") }
        val a = testCrypto.seal(payload)
        val b = testCrypto.seal(payload)
        assertNotEquals(a, b)
        assertEquals(payload, testCrypto.open(a))
    }

    @Test fun `tampered ciphertext is rejected`() {
        val bytes = pythonVector.decodeBase64()!!.toByteArray()
        bytes[bytes.size - 1] = (bytes[bytes.size - 1].toInt() xor 1).toByte()
        val tampered = bytes.toByteString().base64()
        assertFailsWith<Exception> { testCrypto.open(tampered) }
    }

    @Test fun `short or non-base64 input is rejected`() {
        assertFailsWith<IllegalArgumentException> { testCrypto.open("AAAA") }
        assertFailsWith<IllegalArgumentException> { testCrypto.open("not base64 !!") }
    }

    @Test fun `fromHex validates length and characters`() {
        assertFailsWith<IllegalArgumentException> { WireCrypto.fromHex("11".repeat(31)) }
        assertFailsWith<IllegalArgumentException> { WireCrypto.fromHex("zz".repeat(32)) }
        WireCrypto.fromHex("aB".repeat(32)) // mixed case is fine
    }
}
```

- [ ] **Step 4: Run to verify they fail**

Run: `./gradlew test`
Expected: compilation FAIL — `Unresolved reference: WireCrypto`.

- [ ] **Step 5: Implement** `src/main/kotlin/io/github/gekkotron/athena/gateway/client/internal/WireCrypto.kt`:

```kotlin
package io.github.gekkotron.athena.gateway.client.internal

import java.security.SecureRandom
import javax.crypto.Cipher
import javax.crypto.spec.GCMParameterSpec
import javax.crypto.spec.SecretKeySpec
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.jsonObject
import okio.ByteString.Companion.decodeBase64
import okio.ByteString.Companion.toByteString

/** The gateway wire format: base64(nonce[12] ‖ AES-256-GCM ciphertext+tag) of a JSON object. */
internal class WireCrypto(key: ByteArray, private val random: SecureRandom = SecureRandom()) {
    init {
        require(key.size == 32) { "secretKey must be 64 hex characters (32 bytes)" }
    }

    private val keySpec = SecretKeySpec(key.copyOf(), "AES")

    fun seal(payload: JsonObject): String {
        val nonce = ByteArray(NONCE_BYTES).also(random::nextBytes)
        val cipher = Cipher.getInstance(TRANSFORMATION)
        cipher.init(Cipher.ENCRYPT_MODE, keySpec, GCMParameterSpec(TAG_BITS, nonce))
        val ciphertext = cipher.doFinal(payload.toString().encodeToByteArray())
        return (nonce + ciphertext).toByteString().base64()
    }

    fun open(data: String): JsonObject {
        val raw = requireNotNull(data.trim().decodeBase64()) { "response is not base64" }.toByteArray()
        require(raw.size >= NONCE_BYTES + TAG_BITS / 8) { "response is too short" }
        val cipher = Cipher.getInstance(TRANSFORMATION)
        cipher.init(Cipher.DECRYPT_MODE, keySpec, GCMParameterSpec(TAG_BITS, raw, 0, NONCE_BYTES))
        val plaintext = cipher.doFinal(raw, NONCE_BYTES, raw.size - NONCE_BYTES)
        return Json.parseToJsonElement(plaintext.decodeToString()).jsonObject
    }

    companion object {
        private const val TRANSFORMATION = "AES/GCM/NoPadding"
        private const val NONCE_BYTES = 12
        private const val TAG_BITS = 128

        fun fromHex(hex: String): WireCrypto {
            require(hex.length == 64 && hex.all { Character.digit(it, 16) >= 0 }) {
                "secretKey must be 64 hex characters (32 bytes)"
            }
            return WireCrypto(ByteArray(32) { i -> hex.substring(2 * i, 2 * i + 2).toInt(16).toByte() })
        }
    }
}
```

Note: okio's `decodeBase64()` returns `null` for invalid input; `"not base64 !!"` must hit the `requireNotNull`. If okio happens to decode it leniently, the size check still throws `IllegalArgumentException`.

- [ ] **Step 6: Run tests**

Run: `./gradlew test`
Expected: `WireCryptoTest` — 6 tests PASS.

- [ ] **Step 7: Commit**

```bash
git add sdk/kotlin
git -c user.name=Gekkotron -c user.email=60887050+Gekkotron@users.noreply.github.com commit -m "feat(sdk): scaffold Kotlin client project and wire crypto

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

---

### Task 3: Public models, exceptions, `RequestBuilder`

**Files:**
- Create: `src/main/kotlin/io/github/gekkotron/athena/gateway/client/Models.kt`
- Create: `src/main/kotlin/io/github/gekkotron/athena/gateway/client/GatewayException.kt`
- Create: `src/main/kotlin/io/github/gekkotron/athena/gateway/client/internal/RequestBuilder.kt`
- Test: `ModelsTest.kt`, `RequestBuilderTest.kt`

**Interfaces:**
- Consumes: `WireCrypto.seal(JsonObject): String` (Task 2).
- Produces:
  - `public data class GatewayResponse(val status: Int, val body: JsonElement)` with `inline fun <reified T> bodyAs(json: Json = GatewayJson): T`
  - `public data class PublishResult(val topic: String)`
  - `public data class Broker(val host: String? = null, val port: Int? = null, val username: String? = null, val password: String? = null)`
  - `public data class Backoff(val initial: Duration = 1.seconds, val max: Duration = 60.seconds, val factor: Double = 2.0)` with `internal fun delayFor(attempt: Int): Duration`
  - `public sealed interface MqttEvent { Connected(topics: List<String>); Message(topic: String, payload: JsonElement, qos: Int, retain: Boolean, timestamp: Long) }`
  - `public sealed class GatewayException` with `Unauthorized`, `BadRequest`, `Rejected(reason)`, `GatewayError(reason)`, `Upstream(status, body)`, `StreamError(reason)`, `Transport(cause: IOException)`; `internal open val retryable: Boolean` (true only for `StreamError`, `Transport`).
  - `internal class RequestBuilder(crypto: WireCrypto, clock: () -> Long)` with `http(url, method, headers, body, timeoutSeconds): String`, `publish(topic, message, qos, retain, broker): String`, `subscribe(topics: List<String>, qos, broker): String`.

- [ ] **Step 1: Write the failing tests**

`ModelsTest.kt`:

```kotlin
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
```

`RequestBuilderTest.kt`:

```kotlin
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
```

- [ ] **Step 2: Run to verify they fail**

Run: `./gradlew test`
Expected: compilation FAIL — unresolved `GatewayResponse`, `Backoff`, `RequestBuilder`, …

- [ ] **Step 3: Implement**

`Models.kt`:

```kotlin
package io.github.gekkotron.athena.gateway.client

import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.decodeFromJsonElement

/** Lenient decoder used by [GatewayResponse.bodyAs]. */
@PublishedApi
internal val GatewayJson: Json = Json { ignoreUnknownKeys = true }

/** Response of a request forwarded through `/gateway`. */
public data class GatewayResponse(public val status: Int, public val body: JsonElement) {
    /** Decodes [body] into a `@Serializable` type. */
    public inline fun <reified T> bodyAs(json: Json = GatewayJson): T = json.decodeFromJsonElement(body)
}

/** A successful MQTT publish. */
public data class PublishResult(public val topic: String)

/** Per-call MQTT broker overrides; `null` fields use the gateway's defaults. */
public data class Broker(
    public val host: String? = null,
    public val port: Int? = null,
    public val username: String? = null,
    public val password: String? = null,
)

/** Exponential reconnect delay for [AthenaGatewayClient.subscribe]. */
public data class Backoff(
    public val initial: Duration = 1.seconds,
    public val max: Duration = 60.seconds,
    public val factor: Double = 2.0,
) {
    init {
        require(initial.isPositive()) { "initial must be > 0" }
        require(max >= initial) { "max must be >= initial" }
        require(factor >= 1.0) { "factor must be >= 1" }
    }

    internal fun delayFor(attempt: Int): Duration {
        var d = initial
        repeat(attempt) {
            d *= factor
            if (d >= max) return max
        }
        return d
    }
}

/** Events emitted by [AthenaGatewayClient.subscribe]. */
public sealed interface MqttEvent {
    /** The gateway connected to the broker and subscribed to [topics]. */
    public data class Connected(public val topics: List<String>) : MqttEvent

    /** A message on [topic]; [payload] is parsed JSON, or a JSON string for text payloads. */
    public data class Message(
        public val topic: String,
        public val payload: JsonElement,
        public val qos: Int,
        public val retain: Boolean,
        public val timestamp: Long,
    ) : MqttEvent
}
```

`GatewayException.kt`:

```kotlin
package io.github.gekkotron.athena.gateway.client

import java.io.IOException
import kotlinx.serialization.json.JsonElement

/** Every failure surfaced by [AthenaGatewayClient]. */
public sealed class GatewayException(message: String, cause: Throwable? = null) : Exception(message, cause) {
    internal open val retryable: Boolean get() = false

    /** HTTP 401: no secret on the gateway matched this key. */
    public class Unauthorized : GatewayException("No gateway secret matched this key")

    /** HTTP 400: the gateway could not parse the request. */
    public class BadRequest : GatewayException("Gateway could not parse the request")

    /** The gateway refused the request (expired, replayed, port or destination not allowed). */
    public class Rejected(public val reason: String) : GatewayException(reason)

    /** The gateway failed internally, or answered with something unexpected. */
    public class GatewayError(public val reason: String) : GatewayException(reason)

    /** The target service (HTTP device or MQTT broker) returned a failure. */
    public class Upstream(public val status: Int, public val body: JsonElement) :
        GatewayException("Upstream returned $status")

    /** The subscription stream reported an error. */
    public class StreamError(public val reason: String) : GatewayException(reason) {
        override val retryable: Boolean get() = true
    }

    /** Network failure talking to the gateway. */
    public class Transport(cause: IOException) : GatewayException(cause.message ?: "I/O error", cause) {
        override val retryable: Boolean get() = true
    }
}
```

`internal/RequestBuilder.kt`:

```kotlin
package io.github.gekkotron.athena.gateway.client.internal

import io.github.gekkotron.athena.gateway.client.Broker
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive

/** Builds and seals one request payload. Call it again for every attempt: nonces are never reused. */
internal class RequestBuilder(private val crypto: WireCrypto, private val clock: () -> Long) {

    fun http(url: String, method: String, headers: Map<String, String>, body: JsonElement?, timeoutSeconds: Int?): String {
        require(body == null || body is JsonObject || (body is JsonPrimitive && body.isString)) {
            "body must be a JsonObject or a JSON string"
        }
        return seal(buildMap {
            put("url", JsonPrimitive(url))
            put("method", JsonPrimitive(method.uppercase()))
            if (headers.isNotEmpty()) put("headers", JsonObject(headers.mapValues { JsonPrimitive(it.value) }))
            if (body != null) put("body", body)
            if (timeoutSeconds != null) put("timeout", JsonPrimitive(timeoutSeconds))
        })
    }

    fun publish(topic: String, message: String, qos: Int, retain: Boolean, broker: Broker?): String {
        requireQos(qos)
        return seal(buildMap {
            put("topic", JsonPrimitive(topic))
            put("message", JsonPrimitive(message))
            put("qos", JsonPrimitive(qos))
            put("retain", JsonPrimitive(retain))
            putBroker(broker)
        })
    }

    fun subscribe(topics: List<String>, qos: Int, broker: Broker?): String {
        requireQos(qos)
        return seal(buildMap {
            put("topics", JsonArray(topics.map { JsonPrimitive(it) }))
            put("qos", JsonPrimitive(qos))
            putBroker(broker)
        })
    }

    private fun requireQos(qos: Int) = require(qos in 0..2) { "qos must be 0, 1 or 2" }

    private fun MutableMap<String, JsonElement>.putBroker(broker: Broker?) {
        if (broker == null) return
        broker.host?.let { put("broker_host", JsonPrimitive(it)) }
        broker.port?.let { put("broker_port", JsonPrimitive(it)) }
        broker.username?.let { put("username", JsonPrimitive(it)) }
        broker.password?.let { put("password", JsonPrimitive(it)) }
    }

    private fun seal(fields: Map<String, JsonElement>): String =
        crypto.seal(JsonObject(fields + ("timestamp" to JsonPrimitive(clock()))))
}
```

- [ ] **Step 4: Run tests**

Run: `./gradlew test`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add sdk/kotlin
git -c user.name=Gekkotron -c user.email=60887050+Gekkotron@users.noreply.github.com commit -m "feat(sdk): public models, exceptions and request builder

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

---

### Task 4: `ResponseDecoder`

**Files:**
- Create: `src/main/kotlin/io/github/gekkotron/athena/gateway/client/internal/ResponseDecoder.kt`
- Test: `ResponseDecoderTest.kt`

**Interfaces:**
- Consumes: `WireCrypto.open` (Task 2); models + exceptions (Task 3).
- Produces:
  - `internal data class RawResponse(val code: Int, val contentType: String?, val body: String)`
  - `internal sealed interface Frame { Event(event: MqttEvent); Error(reason: String); Disconnected; Ignored }`
  - `internal class ResponseDecoder(crypto: WireCrypto)` with `http(raw, throwOnUpstreamError): GatewayResponse`, `publish(raw): PublishResult`, `streamRejection(raw): Nothing`, `frame(data: String): Frame`.

- [ ] **Step 1: Write the failing tests** — `ResponseDecoderTest.kt`:

```kotlin
package io.github.gekkotron.athena.gateway.client

import io.github.gekkotron.athena.gateway.client.internal.Frame
import io.github.gekkotron.athena.gateway.client.internal.RawResponse
import io.github.gekkotron.athena.gateway.client.internal.ResponseDecoder
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.add
import kotlinx.serialization.json.buildJsonArray
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs

internal fun envelope(status: Int, body: JsonElement, source: String? = null): RawResponse =
    RawResponse(200, "application/octet-stream", sealed {
        put("status", status); put("body", body); put("timestamp", 0)
        if (source != null) put("source", source)
    })

class ResponseDecoderTest {
    private val decoder = ResponseDecoder(testCrypto)
    private val json = buildJsonObject { put("on", true) }

    @Test fun `plaintext 401 and 400`() {
        assertFailsWith<GatewayException.Unauthorized> { decoder.http(RawResponse(401, null, ""), true) }
        assertFailsWith<GatewayException.BadRequest> { decoder.http(RawResponse(400, null, "{}"), true) }
    }

    @Test fun `other plaintext status is a gateway error`() {
        val e = assertFailsWith<GatewayException.GatewayError> { decoder.http(RawResponse(502, null, "bad gateway"), true) }
        assertEquals("HTTP 502", e.reason)
    }

    @Test fun `undecryptable 200 is a gateway error`() {
        val e = assertFailsWith<GatewayException.GatewayError> { decoder.http(RawResponse(200, null, "Zm9vYmFyYmF6cXV4cXV1eHh4eHh4eHh4eHh4eHh4"), true) }
        assertEquals("Response could not be decrypted", e.reason)
    }

    @Test fun `http success returns status and body as-is`() {
        assertEquals(GatewayResponse(200, json), decoder.http(envelope(200, json), true))
        assertEquals(GatewayResponse(200, JsonPrimitive("plain text")), decoder.http(envelope(200, JsonPrimitive("plain text")), true))
        val array = buildJsonArray { add(1) }
        assertEquals(GatewayResponse(200, array), decoder.http(envelope(200, array), true))
    }

    @Test fun `upstream failure throws or is returned`() {
        val e = assertFailsWith<GatewayException.Upstream> { decoder.http(envelope(404, JsonPrimitive("nope")), true) }
        assertEquals(404, e.status)
        assertEquals(GatewayResponse(404, JsonPrimitive("nope")), decoder.http(envelope(404, JsonPrimitive("nope")), false))
    }

    @Test fun `gateway 403 with string body keeps the exact reason`() {
        // /gateway encodes its error body as a JSON string; a skewed phone clock produces this.
        val raw = envelope(403, JsonPrimitive("""{"error": "Request expired"}"""), source = "gateway")
        assertEquals("Request expired", assertFailsWith<GatewayException.Rejected> { decoder.http(raw, false) }.reason)
    }

    @Test fun `gateway 403 with object body and gateway 500`() {
        val rejected = envelope(403, buildJsonObject { put("error", "port not allowed for this secret") }, "gateway")
        assertEquals("port not allowed for this secret", assertFailsWith<GatewayException.Rejected> { decoder.publish(rejected) }.reason)
        val crashed = envelope(500, JsonPrimitive("""{"error": "boom"}"""), "gateway")
        assertEquals("boom", assertFailsWith<GatewayException.GatewayError> { decoder.http(crashed, false) }.reason)
    }

    @Test fun `publish success and broker failure`() {
        val ok = envelope(200, JsonPrimitive("""{"success": true, "topic": "t", "message": "Published successfully"}"""))
        assertEquals(PublishResult("t"), decoder.publish(ok))
        val failed = envelope(500, JsonPrimitive("""{"success": false, "topic": "t", "message": "Failed with code 4"}"""))
        val e = assertFailsWith<GatewayException.Upstream> { decoder.publish(failed) }
        assertEquals(500, e.status)
        assertEquals(JsonPrimitive("Failed with code 4"), (e.body as kotlinx.serialization.json.JsonObject)["message"])
    }

    @Test fun `stream rejection maps gateway errors and never returns`() {
        assertFailsWith<GatewayException.Rejected> {
            decoder.streamRejection(envelope(403, buildJsonObject { put("error", "Request replayed") }, "gateway"))
        }
        assertFailsWith<GatewayException.Unauthorized> { decoder.streamRejection(RawResponse(401, null, "")) }
        assertFailsWith<GatewayException.GatewayError> { decoder.streamRejection(envelope(200, json)) }
    }

    @Test fun `frames decode into events`() {
        val connected = decoder.frame(sealed { put("type", "connected"); put("topic", "a"); put("topics", buildJsonArray { add("a"); add("b") }) })
        assertEquals(Frame.Event(MqttEvent.Connected(listOf("a", "b"))), connected)

        val legacy = decoder.frame(sealed { put("type", "connected"); put("topic", "a") })
        assertEquals(Frame.Event(MqttEvent.Connected(listOf("a"))), legacy)

        val msg = decoder.frame(sealed {
            put("type", "message"); put("topic", "a"); put("payload", "22.5 C"); put("qos", 1); put("retain", true); put("timestamp", 7)
        })
        assertEquals(Frame.Event(MqttEvent.Message("a", JsonPrimitive("22.5 C"), 1, true, 7)), msg)

        assertEquals(Frame.Error("Connection failed with code 5"), decoder.frame(sealed { put("type", "error"); put("message", "Connection failed with code 5") }))
        assertEquals(Frame.Disconnected, decoder.frame(sealed { put("type", "disconnected"); put("message", "bye") }))
        assertEquals(Frame.Ignored, decoder.frame(sealed { put("type", "future-type") }))
    }

    @Test fun `undecryptable frame is a gateway error`() {
        assertIs<GatewayException.GatewayError>(runCatching { decoder.frame("AAAA") }.exceptionOrNull())
    }
}
```

- [ ] **Step 2: Run to verify they fail**

Run: `./gradlew test`
Expected: compilation FAIL — unresolved `ResponseDecoder`, `RawResponse`, `Frame`.

- [ ] **Step 3: Implement** `internal/ResponseDecoder.kt`:

```kotlin
package io.github.gekkotron.athena.gateway.client.internal

import io.github.gekkotron.athena.gateway.client.GatewayException
import io.github.gekkotron.athena.gateway.client.GatewayResponse
import io.github.gekkotron.athena.gateway.client.MqttEvent
import io.github.gekkotron.athena.gateway.client.PublishResult
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.booleanOrNull
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.intOrNull
import kotlinx.serialization.json.longOrNull

/** What the transport got back: status code, content type, and body text. */
internal data class RawResponse(val code: Int, val contentType: String?, val body: String)

/** One decoded SSE frame of `/mqtt/subscribe`. */
internal sealed interface Frame {
    data class Event(val event: MqttEvent) : Frame
    data class Error(val reason: String) : Frame
    data object Disconnected : Frame
    data object Ignored : Frame
}

internal class ResponseDecoder(private val crypto: WireCrypto) {

    private class Envelope(val status: Int, val body: JsonElement)

    fun http(raw: RawResponse, throwOnUpstreamError: Boolean): GatewayResponse {
        val env = envelope(raw)
        if (env.status !in 200..299 && throwOnUpstreamError) throw GatewayException.Upstream(env.status, env.body)
        return GatewayResponse(env.status, env.body)
    }

    fun publish(raw: RawResponse): PublishResult {
        val env = envelope(raw)
        val body = env.body.parsedIfJsonString()
        val obj = body as? JsonObject
        val topic = obj.string("topic")
        if (env.status != 200 || obj?.get("success").boolean() != true || topic == null) {
            throw GatewayException.Upstream(env.status, body)
        }
        return PublishResult(topic)
    }

    /** `/mqtt/subscribe` answered without an event stream: always a failure. */
    fun streamRejection(raw: RawResponse): Nothing {
        val env = envelope(raw)
        throw GatewayException.GatewayError("Expected an event stream, got status ${env.status}")
    }

    fun frame(data: String): Frame {
        val obj = open(data)
        return when (obj.string("type")) {
            "connected" -> Frame.Event(MqttEvent.Connected(
                (obj["topics"] as? JsonArray)?.mapNotNull { (it as? JsonPrimitive)?.contentOrNull }
                    ?: listOfNotNull(obj.string("topic")),
            ))
            "message" -> Frame.Event(MqttEvent.Message(
                topic = obj.string("topic").orEmpty(),
                payload = obj["payload"] ?: JsonNull,
                qos = (obj["qos"] as? JsonPrimitive)?.intOrNull ?: 0,
                retain = obj["retain"].boolean() ?: false,
                timestamp = (obj["timestamp"] as? JsonPrimitive)?.longOrNull ?: 0L,
            ))
            "error" -> Frame.Error(obj.string("message") ?: "stream error")
            "disconnected" -> Frame.Disconnected
            else -> Frame.Ignored
        }
    }

    private fun envelope(raw: RawResponse): Envelope {
        when (raw.code) {
            200 -> Unit
            401 -> throw GatewayException.Unauthorized()
            400 -> throw GatewayException.BadRequest()
            else -> throw GatewayException.GatewayError("HTTP ${raw.code}")
        }
        val obj = open(raw.body)
        val status = (obj["status"] as? JsonPrimitive)?.intOrNull
            ?: throw GatewayException.GatewayError("Response has no status")
        val body = obj["body"] ?: JsonNull
        if (obj.string("source") == "gateway") {
            val reason = body.parsedIfJsonString().errorMessage()
            throw if (status == 403) GatewayException.Rejected(reason) else GatewayException.GatewayError(reason)
        }
        return Envelope(status, body)
    }

    private fun open(data: String): JsonObject = try {
        crypto.open(data)
    } catch (e: Exception) {
        throw GatewayException.GatewayError("Response could not be decrypted")
    }
}

private fun JsonObject?.string(key: String): String? = (this?.get(key) as? JsonPrimitive)?.contentOrNull

private fun JsonElement?.boolean(): Boolean? = (this as? JsonPrimitive)?.booleanOrNull

/** The gateway JSON-encodes some bodies as a string; decode those, leave everything else alone. */
private fun JsonElement.parsedIfJsonString(): JsonElement =
    if (this is JsonPrimitive && isString) runCatching { Json.parseToJsonElement(content) }.getOrDefault(this) else this

private fun JsonElement.errorMessage(): String =
    ((this as? JsonObject)?.get("error") as? JsonPrimitive)?.contentOrNull ?: toString()
```

- [ ] **Step 4: Run tests**

Run: `./gradlew test`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add sdk/kotlin
git -c user.name=Gekkotron -c user.email=60887050+Gekkotron@users.noreply.github.com commit -m "feat(sdk): decode gateway responses and SSE frames

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

---

### Task 5: `Transport` and `OkHttpTransport`

**Files:**
- Create: `src/main/kotlin/io/github/gekkotron/athena/gateway/client/internal/Transport.kt`
- Test: `OkHttpTransportTest.kt`

**Interfaces:**
- Consumes: `RawResponse` (Task 4), `GatewayException.Transport` (Task 3).
- Produces:
  - `internal sealed interface StreamItem { Data(data: String); NotAStream(response: RawResponse) }`
  - `internal interface Transport { suspend fun post(path: String, body: String): RawResponse; fun stream(path: String, body: String): Flow<StreamItem> }` — both throw `GatewayException.Transport` on I/O failure.
  - `internal class OkHttpTransport(baseUrl: String, client: OkHttpClient) : Transport` with `internal fun urlFor(path: String): HttpUrl` and `internal val streamClient: OkHttpClient` (read timeout 45 s).

- [ ] **Step 1: Write the failing tests** — `OkHttpTransportTest.kt` (uses `runBlocking`, not `runTest`: MockWebServer is real I/O and `runTest`'s virtual time would fire timeouts instantly):

```kotlin
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
        // 9 bytes = "data: A\n\n", then the server stalls for a day.
        server.enqueue(
            MockResponse().setHeader("Content-Type", "text/event-stream")
                .setBody("data: A\n\ndata: B\n\n").throttleBody(9, 1, TimeUnit.DAYS),
        )
        val first = withTimeout(5_000) { transport.stream("mqtt/subscribe", "B").first() }
        assertEquals(StreamItem.Data("A"), first)
    }

    @Test fun `stream read timeout outlasts the 15 s server keepalive`() {
        assertEquals(45_000, transport.streamClient.readTimeoutMillis)
    }
}
```

- [ ] **Step 2: Run to verify they fail**

Run: `./gradlew test --tests '*OkHttpTransportTest*'`
Expected: compilation FAIL — unresolved `OkHttpTransport`, `StreamItem`.

- [ ] **Step 3: Implement** `internal/Transport.kt`:

```kotlin
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
    /** @throws GatewayException.Transport on I/O failure. */
    suspend fun post(path: String, body: String): RawResponse

    /** Cold flow; each collection opens one connection. Ends when the server closes it. */
    fun stream(path: String, body: String): Flow<StreamItem>
}

internal class OkHttpTransport(baseUrl: String, private val client: OkHttpClient) : Transport {
    private val base: HttpUrl = baseUrl.toHttpUrl()

    /** Streams sit idle between messages; the server sends a keepalive every 15 s. */
    internal val streamClient: OkHttpClient = client.newBuilder().readTimeout(45, TimeUnit.SECONDS).build()

    internal fun urlFor(path: String): HttpUrl = base.newBuilder().addPathSegments(path).build()

    override suspend fun post(path: String, body: String): RawResponse = suspendCancellableCoroutine { cont ->
        val call = client.newCall(request(path, body))
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
```

- [ ] **Step 4: Run tests**

Run: `./gradlew test`
Expected: all PASS (the cancellation test finishes in well under 5 s).

- [ ] **Step 5: Commit**

```bash
git add sdk/kotlin
git -c user.name=Gekkotron -c user.email=60887050+Gekkotron@users.noreply.github.com commit -m "feat(sdk): OkHttp transport with in-house SSE reader

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

---

### Task 6: `AthenaGatewayClient` — `http` and `publish`

**Files:**
- Create: `src/main/kotlin/io/github/gekkotron/athena/gateway/client/AthenaGatewayClient.kt`
- Create: `src/test/kotlin/io/github/gekkotron/athena/gateway/client/FakeTransport.kt`
- Test: `AthenaGatewayClientTest.kt`

**Interfaces:**
- Consumes: `Transport`, `StreamItem` (Task 5); `RequestBuilder` (Task 3); `ResponseDecoder`, `RawResponse` (Task 4); `WireCrypto.fromHex` (Task 2).
- Produces:
  - `public class AthenaGatewayClient(baseUrl: String, secretKey: String, okHttpClient: OkHttpClient = OkHttpClient(), clock: () -> Long = …)` and `internal constructor(transport: Transport, crypto: WireCrypto, clock: () -> Long)`.
  - `public suspend fun http(url, method = "GET", headers = emptyMap(), body: JsonElement? = null, timeoutSeconds: Int? = null, throwOnUpstreamError = true): GatewayResponse`
  - `public suspend fun publish(topic, message, qos = 0, retain = false, broker: Broker? = null): PublishResult`
  - Test double `FakeTransport` (used again by Task 7).

- [ ] **Step 1: Write the failing tests**

`FakeTransport.kt`:

```kotlin
package io.github.gekkotron.athena.gateway.client

import io.github.gekkotron.athena.gateway.client.internal.RawResponse
import io.github.gekkotron.athena.gateway.client.internal.StreamItem
import io.github.gekkotron.athena.gateway.client.internal.Transport
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.emptyFlow

internal class FakeTransport : Transport {
    val posts = mutableListOf<Pair<String, String>>()
    val streams = mutableListOf<Pair<String, String>>()
    var postReply: (path: String) -> RawResponse = { error("no post reply configured") }
    val streamReplies = ArrayDeque<Flow<StreamItem>>()

    override suspend fun post(path: String, body: String): RawResponse {
        posts += path to body
        return postReply(path)
    }

    override fun stream(path: String, body: String): Flow<StreamItem> {
        streams += path to body
        return streamReplies.removeFirstOrNull() ?: emptyFlow()
    }
}
```

`AthenaGatewayClientTest.kt`:

```kotlin
package io.github.gekkotron.athena.gateway.client

import kotlinx.coroutines.test.runTest
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class AthenaGatewayClientTest {
    private val transport = FakeTransport()
    private val client = AthenaGatewayClient(transport, testCrypto) { 42 }

    @Test fun `public constructor validates key and url eagerly`() {
        assertFailsWith<IllegalArgumentException> { AthenaGatewayClient("https://h.example", "abc") }
        assertFailsWith<IllegalArgumentException> { AthenaGatewayClient("nope", TEST_KEY) }
        AthenaGatewayClient("https://h.example", TEST_KEY)
    }

    @Test fun `http posts a sealed request to gateway and decodes the reply`() = runTest {
        val state = buildJsonObject { put("on", true) }
        transport.postReply = { envelope(200, state) }
        assertEquals(GatewayResponse(200, state), client.http("http://10.0.0.1/state"))
        val (path, body) = transport.posts.single()
        assertEquals("gateway", path)
        assertEquals(
            buildJsonObject { put("url", "http://10.0.0.1/state"); put("method", "GET"); put("timestamp", 42) },
            opened(body),
        )
    }

    @Test fun `http upstream error can be returned instead of thrown`() = runTest {
        transport.postReply = { envelope(404, JsonPrimitive("nope")) }
        assertFailsWith<GatewayException.Upstream> { client.http("http://h") }
        assertEquals(404, client.http("http://h", throwOnUpstreamError = false).status)
    }

    @Test fun `publish posts to mqtt publish`() = runTest {
        transport.postReply = { envelope(200, JsonPrimitive("""{"success": true, "topic": "t", "message": "ok"}""")) }
        assertEquals(PublishResult("t"), client.publish("t", "on", qos = 1))
        assertEquals("mqtt/publish", transport.posts.single().first)
        assertEquals(JsonPrimitive(1), opened(transport.posts.single().second)["qos"])
    }

    @Test fun `every call is freshly encrypted`() = runTest {
        transport.postReply = { envelope(200, JsonPrimitive("""{"success": true, "topic": "t", "message": "ok"}""")) }
        client.publish("t", "m"); client.publish("t", "m")
        assertEquals(2, transport.posts.map { it.second }.toSet().size)
    }
}
```

- [ ] **Step 2: Run to verify they fail**

Run: `./gradlew test --tests '*AthenaGatewayClientTest*'`
Expected: compilation FAIL — unresolved `AthenaGatewayClient`.

- [ ] **Step 3: Implement** `AthenaGatewayClient.kt` (subscribe is added in Task 7):

```kotlin
package io.github.gekkotron.athena.gateway.client

import io.github.gekkotron.athena.gateway.client.internal.OkHttpTransport
import io.github.gekkotron.athena.gateway.client.internal.RequestBuilder
import io.github.gekkotron.athena.gateway.client.internal.ResponseDecoder
import io.github.gekkotron.athena.gateway.client.internal.Transport
import io.github.gekkotron.athena.gateway.client.internal.WireCrypto
import kotlinx.serialization.json.JsonElement
import okhttp3.OkHttpClient

/**
 * Client for an Athena HTTP/MQTT gateway.
 *
 * @param baseUrl gateway root, e.g. `https://myhost.tailnet-name.ts.net` (a path prefix is allowed).
 * @param secretKey the 64-hex-character secret configured on the gateway.
 * @param okHttpClient bring your own client for certificate pinning, proxies or timeouts.
 * @param clock unix seconds used for request timestamps; override only in tests.
 * @throws IllegalArgumentException if [baseUrl] or [secretKey] is malformed.
 */
public class AthenaGatewayClient internal constructor(
    private val transport: Transport,
    crypto: WireCrypto,
    clock: () -> Long,
) {
    public constructor(
        baseUrl: String,
        secretKey: String,
        okHttpClient: OkHttpClient = OkHttpClient(),
        clock: () -> Long = { System.currentTimeMillis() / 1000 },
    ) : this(OkHttpTransport(baseUrl, okHttpClient), WireCrypto.fromHex(secretKey), clock)

    private val requests = RequestBuilder(crypto, clock)
    private val decoder = ResponseDecoder(crypto)

    /**
     * Forwards an HTTP request to [url] through the gateway.
     *
     * @param body a `JsonObject` (sent as JSON) or a JSON string (sent raw).
     * @param throwOnUpstreamError when false, non-2xx upstream answers are returned instead of thrown.
     * @throws GatewayException
     */
    public suspend fun http(
        url: String,
        method: String = "GET",
        headers: Map<String, String> = emptyMap(),
        body: JsonElement? = null,
        timeoutSeconds: Int? = null,
        throwOnUpstreamError: Boolean = true,
    ): GatewayResponse = decoder.http(
        transport.post("gateway", requests.http(url, method, headers, body, timeoutSeconds)),
        throwOnUpstreamError,
    )

    /**
     * Publishes [message] to [topic].
     *
     * @throws GatewayException
     */
    public suspend fun publish(
        topic: String,
        message: String,
        qos: Int = 0,
        retain: Boolean = false,
        broker: Broker? = null,
    ): PublishResult = decoder.publish(
        transport.post("mqtt/publish", requests.publish(topic, message, qos, retain, broker)),
    )
}
```

- [ ] **Step 4: Run tests**

Run: `./gradlew test`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add sdk/kotlin
git -c user.name=Gekkotron -c user.email=60887050+Gekkotron@users.noreply.github.com commit -m "feat(sdk): AthenaGatewayClient http and publish

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

---

### Task 7: `subscribe` with opt-in reconnect

**Files:**
- Modify: `src/main/kotlin/io/github/gekkotron/athena/gateway/client/AthenaGatewayClient.kt`
- Test: `SubscribeTest.kt`

**Interfaces:**
- Consumes: `FakeTransport`, `StreamItem`, `Frame`, `ResponseDecoder.frame/streamRejection`, `Backoff.delayFor`, `GatewayException.retryable`.
- Produces: `public fun subscribe(vararg topics: String, qos: Int = 0, broker: Broker? = null, reconnect: Backoff? = null): Flow<MqttEvent>` — cold; each collection (and each reconnect attempt) builds a new encrypted request.

- [ ] **Step 1: Write the failing tests** — `SubscribeTest.kt`:

```kotlin
package io.github.gekkotron.athena.gateway.client

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
```

- [ ] **Step 2: Run to verify they fail**

Run: `./gradlew test --tests '*SubscribeTest*'`
Expected: compilation FAIL — unresolved `subscribe`.

- [ ] **Step 3: Implement** — add these imports to `AthenaGatewayClient.kt`:

```kotlin
import io.github.gekkotron.athena.gateway.client.internal.Frame
import io.github.gekkotron.athena.gateway.client.internal.StreamItem
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.emitAll
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.onCompletion
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.flow.retryWhen
import kotlinx.coroutines.flow.transformWhile
```

and these members inside the class:

```kotlin
    /**
     * Streams MQTT [topics] live. Cold: collection opens the stream, cancellation closes it.
     *
     * Without [reconnect] the flow completes when the gateway reports a disconnect and fails on
     * any error. With [reconnect] it re-opens the stream after disconnects, stream errors and
     * network failures, waiting per [Backoff]; `Unauthorized`, `BadRequest` and `Rejected` are
     * never retried. Every attempt sends a newly encrypted request.
     *
     * @throws IllegalArgumentException if no topic is given.
     */
    public fun subscribe(
        vararg topics: String,
        qos: Int = 0,
        broker: Broker? = null,
        reconnect: Backoff? = null,
    ): Flow<MqttEvent> {
        require(topics.isNotEmpty()) { "subscribe needs at least one topic" }
        val once = streamOnce(topics.toList(), qos, broker)
        val backoff = reconnect ?: return once
        return flow {
            var failures = 0
            emitAll(
                once
                    .onEach { if (it is MqttEvent.Connected) failures = 0 }
                    .onCompletion { cause -> if (cause == null) throw StreamEnded }
                    .retryWhen { cause, _ ->
                        val retry = cause === StreamEnded || (cause is GatewayException && cause.retryable)
                        if (retry) delay(backoff.delayFor(failures++))
                        retry
                    },
            )
        }
    }

    /** One connection attempt; the request is built at collection time so it is never reused. */
    private fun streamOnce(topics: List<String>, qos: Int, broker: Broker?): Flow<MqttEvent> = flow {
        val body = requests.subscribe(topics, qos, broker)
        emitAll(
            transport.stream("mqtt/subscribe", body).transformWhile { item ->
                when (item) {
                    is StreamItem.NotAStream -> decoder.streamRejection(item.response)
                    is StreamItem.Data -> when (val frame = decoder.frame(item.data)) {
                        is Frame.Event -> { emit(frame.event); true }
                        is Frame.Error -> throw GatewayException.StreamError(frame.reason)
                        Frame.Disconnected -> false
                        Frame.Ignored -> true
                    }
                }
            },
        )
    }

    /** Internal signal: a stream ended normally and should be re-opened. Never escapes. */
    private object StreamEnded : Exception()
```

- [ ] **Step 4: Run tests**

Run: `./gradlew test`
Expected: all PASS. If `retryWhen` reports a flow exception-transparency violation, the `onCompletion` throw has been placed *after* `retryWhen` — keep the order `onEach → onCompletion → retryWhen`.

- [ ] **Step 5: Commit**

```bash
git add sdk/kotlin
git -c user.name=Gekkotron -c user.email=60887050+Gekkotron@users.noreply.github.com commit -m "feat(sdk): subscribe as a Flow with opt-in reconnect

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

---

### Task 8: Publishing + JitPack config

**Files:**
- Modify: `sdk/kotlin/build.gradle.kts` (append publishing block)
- Create: `jitpack.yml` (repo root)

**Interfaces:**
- Produces: `./gradlew publishToMavenLocal -Pversion=X` writes `com/github/Gekkotron/Athena-HttpMqttGateway/athena-gateway-client/X/` with jar, sources jar, pom.

- [ ] **Step 1: Append to `build.gradle.kts`:**

```kotlin
publishing {
    publications {
        create<MavenPublication>("maven") {
            artifactId = "athena-gateway-client"
            from(components["java"])
            pom {
                name.set("Athena Gateway Client")
                description.set("Kotlin client for the Athena end-to-end encrypted HTTP/MQTT gateway.")
                url.set("https://github.com/Gekkotron/Athena-HttpMqttGateway")
                developers { developer { id.set("Gekkotron"); name.set("Gekkotron") } }
                scm { url.set("https://github.com/Gekkotron/Athena-HttpMqttGateway") }
            }
        }
    }
}
```

- [ ] **Step 2: Create `jitpack.yml`:**

```yaml
# JitPack builds the Kotlin client from sdk/kotlin on the first request for a tag.
jdk:
  - openjdk17
install:
  - cd sdk/kotlin && ./gradlew publishToMavenLocal -Pversion=$VERSION -x test
```

- [ ] **Step 3: Verify the local publish**

```bash
cd sdk/kotlin && ./gradlew publishToMavenLocal -Pversion=0.0.0-local
ls ~/.m2/repository/com/github/Gekkotron/Athena-HttpMqttGateway/athena-gateway-client/0.0.0-local/
```

Expected: `athena-gateway-client-0.0.0-local.jar`, `…-sources.jar`, `….pom`, `….module`. Then check the pom lists okhttp, kotlinx-coroutines-core, kotlinx-serialization-json with `compile` scope:

```bash
grep -A2 "<artifactId>okhttp\|<artifactId>kotlinx" ~/.m2/repository/com/github/Gekkotron/Athena-HttpMqttGateway/athena-gateway-client/0.0.0-local/*.pom
```

Also confirm no `android.` references: `unzip -l …0.0.0-local.jar | grep -c '\.class'` > 0 and `javap -v` is not needed; the compiler would have failed on `android.*` imports.

Finally delete the local test publish: `rm -rf ~/.m2/repository/com/github/Gekkotron/Athena-HttpMqttGateway`.

- [ ] **Step 4: Commit**

```bash
git add sdk/kotlin/build.gradle.kts jitpack.yml
git -c user.name=Gekkotron -c user.email=60887050+Gekkotron@users.noreply.github.com commit -m "build(sdk): maven publication and JitPack config

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

---

### Task 9: Wire-compatibility tests + CI jobs + SDK coverage badge

**Files:**
- Create: `sdk/kotlin/src/test/kotlin/io/github/gekkotron/athena/gateway/client/GatewayIntegrationTest.kt`
- Modify: `.github/workflows/ci.yml`

**Interfaces:**
- Consumes: the public API only (`AthenaGatewayClient`, `Backoff`, `MqttEvent`, `GatewayException`).
- Environment contract (set by CI): `GATEWAY_URL`, `FULL_KEY` (scope `*`), `SCOPED_KEY` (scope `80@127.0.0.1`), `ECHO_URL` (serves `{"ok": true}`). Tests are skipped when `GATEWAY_URL` is unset.

- [ ] **Step 1: Write the integration test**

```kotlin
package io.github.gekkotron.athena.gateway.client

import kotlinx.coroutines.async
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
            full.publish("sdk-it/text", "22.5 C")
            assertEquals(JsonPrimitive("22.5 C"), received.await().payload)
        }
    }
}
```

- [ ] **Step 2: Verify it is skipped locally**

Run: `./gradlew integrationTest`
Expected: BUILD SUCCESSFUL, tests reported as skipped (no `GATEWAY_URL`). `./gradlew test` still does not run them.

- [ ] **Step 3: Run it locally against a real server (manual check before CI)**

```bash
# from the repo root, in three terminals / background jobs:
docker run -d --rm --name sdk-mqtt -p 1883:1883 eclipse-mosquitto:2 mosquitto -c /mosquitto-no-auth.conf
mkdir -p "$S/www" && echo '{"ok": true}' > "$S/www/state.json" && python -m http.server 8000 --bind 127.0.0.1 --directory "$S/www" &
printf '%s:*\n%s:80@127.0.0.1\n' "$(printf '11%.0s' {1..32})" "$(printf '22%.0s' {1..32})" > "$S/it_keys.txt"
SECRET_KEY_FILE="$S/it_keys.txt" MQTT_BROKER_HOST=127.0.0.1 PORT=10000 python -m server &
cd sdk/kotlin && GATEWAY_URL=http://127.0.0.1:10000 FULL_KEY=$(printf '11%.0s' {1..32}) SCOPED_KEY=$(printf '22%.0s' {1..32}) ECHO_URL=http://127.0.0.1:8000/state.json ./gradlew integrationTest
```

Expected: 5 tests PASS. Then stop the three processes (`docker stop sdk-mqtt`, `kill %1 %2`). If Docker is unavailable locally, skip this step and rely on CI (say so in the report).

- [ ] **Step 4: Extend `.github/workflows/ci.yml`** — add two jobs after `test`, and replace the `coverage-badge` job:

```yaml
  sdk-unit:
    runs-on: ubuntu-latest
    defaults:
      run:
        working-directory: sdk/kotlin
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-java@v4
        with:
          distribution: temurin
          java-version: "17"
      - uses: gradle/actions/setup-gradle@v4
      - run: ./gradlew test koverXmlReport koverVerify
      - uses: actions/upload-artifact@v4
        with:
          name: sdk-coverage
          path: sdk/kotlin/build/reports/kover/report.xml

  sdk-integration:
    runs-on: ubuntu-latest
    env:
      GATEWAY_URL: http://127.0.0.1:10000
      ECHO_URL: http://127.0.0.1:8000/state.json
      FULL_KEY: "1111111111111111111111111111111111111111111111111111111111111111"
      SCOPED_KEY: "2222222222222222222222222222222222222222222222222222222222222222"
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"
          cache: pip
          cache-dependency-path: requirements*.txt
      - run: pip install -r requirements.txt
      - uses: actions/setup-java@v4
        with:
          distribution: temurin
          java-version: "17"
      - uses: gradle/actions/setup-gradle@v4
      - name: Start broker, echo server and gateway
        run: |
          docker run -d --name mqtt -p 1883:1883 eclipse-mosquitto:2 mosquitto -c /mosquitto-no-auth.conf
          mkdir -p "$RUNNER_TEMP/www" && echo '{"ok": true}' > "$RUNNER_TEMP/www/state.json"
          nohup python -m http.server 8000 --bind 127.0.0.1 --directory "$RUNNER_TEMP/www" > "$RUNNER_TEMP/echo.log" 2>&1 &
          printf '%s:*\n%s:80@127.0.0.1\n' "$FULL_KEY" "$SCOPED_KEY" > "$RUNNER_TEMP/secret_key.txt"
          SECRET_KEY_FILE="$RUNNER_TEMP/secret_key.txt" MQTT_BROKER_HOST=127.0.0.1 PORT=10000 \
            nohup python -m server > "$RUNNER_TEMP/gateway.log" 2>&1 &
          for i in $(seq 1 30); do curl -sf http://127.0.0.1:10000/health && exit 0; sleep 1; done
          cat "$RUNNER_TEMP/gateway.log"; exit 1
      - run: ./gradlew integrationTest
        working-directory: sdk/kotlin
      - if: failure()
        run: cat "$RUNNER_TEMP/gateway.log"; docker logs mqtt

  # Publishes shields.io endpoint JSON files to the orphan `badges` branch;
  # the README badges read them from there.
  coverage-badge:
    needs: [test, sdk-unit]
    if: github.event_name == 'push' && github.ref == 'refs/heads/master'
    runs-on: ubuntu-latest
    permissions:
      contents: write
    steps:
      - uses: actions/download-artifact@v4
        with:
          name: coverage
          path: py
      - uses: actions/download-artifact@v4
        with:
          name: sdk-coverage
          path: sdk
      - name: Build badge JSON
        run: |
          python3 - <<'PY'
          import json, os
          import xml.etree.ElementTree as ET

          def badge(label, pct, path):
              color = next(c for t, c in [(90, "brightgreen"), (75, "green"),
                                          (60, "yellow"), (40, "orange"), (0, "red")] if pct >= t)
              json.dump({"schemaVersion": 1, "label": label, "message": f"{pct:.0f}%", "color": color},
                        open(path, "w"))

          os.makedirs("out", exist_ok=True)
          badge("coverage", json.load(open("py/coverage.json"))["totals"]["percent_covered"], "out/coverage.json")
          line = next(c for c in ET.parse("sdk/report.xml").getroot().findall("counter") if c.get("type") == "LINE")
          covered, missed = int(line.get("covered")), int(line.get("missed"))
          badge("SDK coverage", 100 * covered / (covered + missed), "out/sdk-coverage.json")
          PY
      - name: Push to badges branch
        env:
          GITHUB_TOKEN: ${{ secrets.GITHUB_TOKEN }}
        run: |
          cd out
          git init -q -b badges
          git add coverage.json sdk-coverage.json
          git -c user.name="github-actions[bot]" \
              -c user.email="41898282+github-actions[bot]@users.noreply.github.com" \
              commit -q -m "coverage badges for ${GITHUB_SHA}"
          git push -q -f "https://x-access-token:${GITHUB_TOKEN}@github.com/${GITHUB_REPOSITORY}.git" badges
```

- [ ] **Step 5: Validate the workflow file**

Run: `python -c "import yaml; print(list(yaml.safe_load(open('.github/workflows/ci.yml'))['jobs']))"`
Expected: `['test', 'sdk-unit', 'sdk-integration', 'coverage-badge']`.

Run: `cd sdk/kotlin && ./gradlew test koverXmlReport koverVerify`
Expected: BUILD SUCCESSFUL (coverage ≥ 85 %); `build/reports/kover/report.xml` exists and its root has a `<counter type="LINE" …>`.

- [ ] **Step 6: Commit**

```bash
git add sdk/kotlin .github/workflows/ci.yml
git -c user.name=Gekkotron -c user.email=60887050+Gekkotron@users.noreply.github.com commit -m "ci(sdk): unit + live wire-compatibility jobs, SDK coverage badge

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

---

### Task 10: Documentation

**Files:**
- Create: `sdk/kotlin/README.md`
- Modify: `README.md` (badge row; replace the hand-written Kotlin client section with a pointer)

- [ ] **Step 1: Write `sdk/kotlin/README.md`:**

````markdown
# Athena Gateway Client (Kotlin)

Kotlin/JVM client for the [Athena HTTP/MQTT gateway](../../README.md): forward HTTP
requests, publish to MQTT and stream MQTT topics live, end-to-end encrypted.
Works in any Android app (no Android framework dependency) and on the JVM.

Requires a gateway at **v1.1.0 or later**.

## Install

```kotlin
// settings.gradle.kts
dependencyResolutionManagement {
    repositories { mavenCentral(); maven("https://jitpack.io") }
}

// build.gradle.kts
dependencies {
    implementation("com.github.Gekkotron.Athena-HttpMqttGateway:athena-gateway-client:v1.1.0")
}
```

## Quick start

```kotlin
val gateway = AthenaGatewayClient(
    baseUrl = "https://myhost.tailnet-name.ts.net",
    secretKey = BuildConfig.GATEWAY_KEY,   // 64 hex chars; never hardcode it in source
)

// Forward an HTTP request to a device on your LAN
val state = gateway.http("http://192.168.1.50/api/state")
println(state.body)

// Publish to MQTT
gateway.publish("home/light/set", "on", qos = 1)

// Stream several topics; cancel the coroutine to close the stream
gateway.subscribe("home/sensors/#", "home/lights/+/state").collect { event ->
    when (event) {
        is MqttEvent.Connected -> println("subscribed to ${event.topics}")
        is MqttEvent.Message -> println("${event.topic}: ${event.payload}")
    }
}
```

`payload` is a `JsonElement`: JSON messages arrive parsed, text messages as a JSON string.
Decode typed bodies with `state.bodyAs<MyState>()` (`@Serializable` class).

## In an Android ViewModel

```kotlin
class SensorsViewModel(private val gateway: AthenaGatewayClient) : ViewModel() {
    val readings: StateFlow<Map<String, JsonElement>> = gateway
        .subscribe("home/sensors/#", reconnect = Backoff())
        .filterIsInstance<MqttEvent.Message>()
        .runningFold(emptyMap<String, JsonElement>()) { acc, m -> acc + (m.topic to m.payload) }
        .stateIn(viewModelScope, SharingStarted.WhileSubscribed(5_000), emptyMap())
}
```

## Reconnecting

`subscribe(..., reconnect = Backoff(initial = 1.seconds, max = 60.seconds))` re-opens the
stream after disconnects, stream errors and network failures, doubling the wait each time
and resetting once connected. Every attempt is a newly encrypted request (the gateway
rejects replays). Without `reconnect`, the flow completes on disconnect and throws on error.

## Errors

All failures are `GatewayException`s:

| Exception | Meaning | Retried by `reconnect` |
|---|---|---|
| `Unauthorized` | No gateway secret matches your key | no |
| `BadRequest` | The gateway could not parse the request | no |
| `Rejected(reason)` | Refused: `Request expired` (check the device clock), `Request replayed`, `port not allowed…`, `destination not allowed…` | no |
| `GatewayError(reason)` | Gateway-side failure | no |
| `Upstream(status, body)` | The device or broker failed (pass `throwOnUpstreamError = false` to `http` to get the response instead) | — |
| `StreamError(reason)` | The live stream reported an error | yes |
| `Transport(cause)` | Network failure | yes |

`http` and `publish` never retry on their own: call them again if you need to.

## Certificate pinning

```kotlin
val pinned = OkHttpClient.Builder()
    .certificatePinner(
        CertificatePinner.Builder()
            .add("myhost.tailnet-name.ts.net", "sha256/<base64 pin>")
            .build(),
    )
    .build()
val gateway = AthenaGatewayClient(baseUrl, key, okHttpClient = pinned)
```

The client keeps your timeouts for `http`/`publish`; streams use a 45 s read timeout
(the gateway sends a keepalive every 15 s).

## Development

```bash
cd sdk/kotlin
./gradlew test koverVerify        # unit tests, coverage >= 85 %
./gradlew integrationTest         # needs a live gateway, see .github/workflows/ci.yml
```
````

- [ ] **Step 2: Update the root `README.md`**

1. Add a third badge under the two existing ones:
   `[![SDK coverage](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/Gekkotron/Athena-HttpMqttGateway/badges/sdk-coverage.json)](sdk/kotlin/README.md)`
2. Find the hand-written Kotlin/Android client section (the one defining `class EncryptedGatewayClient` with OkHttp and `SecretKeySpec`; locate it with `grep -n "EncryptedGatewayClient\|^##" README.md`). Replace the whole section body — from its `###`/`##` heading to the next heading of the same level — with:

~~~markdown
### Kotlin / Android

Use the official client library, [`athena-gateway-client`](sdk/kotlin/README.md):

```kotlin
implementation("com.github.Gekkotron.Athena-HttpMqttGateway:athena-gateway-client:v1.1.0")
```
~~~

   Keep any certificate-pinning *server-side* guidance outside that section untouched.
3. In "Project Structure", add `sdk/kotlin/  Kotlin client library (athena-gateway-client)`.

- [ ] **Step 3: Check links and leftovers**

Run: `grep -n "EncryptedGatewayClient\|tail497f\|geekoma5" README.md sdk/kotlin/README.md`
Expected: no output.

- [ ] **Step 4: Commit**

```bash
git add README.md sdk/kotlin/README.md
git -c user.name=Gekkotron -c user.email=60887050+Gekkotron@users.noreply.github.com commit -m "docs(sdk): client README, root README pointer and badge

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

---

### Task 11: Push, CI green, JitPack pre-release (user-gated)

**Files:** none.

- [ ] **Step 1: Ask the user** for OK to push `master` and to push the tag `v1.1.0-rc1`. Stop until they answer.

- [ ] **Step 2: Push and watch CI**

```bash
git push origin master
gh run watch -R Gekkotron/Athena-HttpMqttGateway --exit-status $(gh run list -R Gekkotron/Athena-HttpMqttGateway --limit 1 --json databaseId -q '.[0].databaseId')
curl -s https://raw.githubusercontent.com/Gekkotron/Athena-HttpMqttGateway/badges/sdk-coverage.json
```

Expected: all four jobs succeed; the SDK badge JSON exists.

- [ ] **Step 3: JitPack pre-release**

```bash
git tag v1.1.0-rc1 && git push origin v1.1.0-rc1
curl -s -m 600 https://jitpack.io/com/github/Gekkotron/Athena-HttpMqttGateway/v1.1.0-rc1/build.log | tail -20
curl -sI https://jitpack.io/com/github/Gekkotron/Athena-HttpMqttGateway/athena-gateway-client/v1.1.0-rc1/athena-gateway-client-v1.1.0-rc1.pom | head -1
```

Expected: build log ends with `BUILD SUCCESSFUL` and lists `athena-gateway-client`; the pom URL returns `HTTP/2 200`. If JitPack serves the artifact under different coordinates, update the install string in `sdk/kotlin/README.md`, the root README and the spec, commit, and report the corrected coordinates to the user.

- [ ] **Step 4: Report** results and ask whether to tag the final `v1.1.0`.
