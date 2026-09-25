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
        is MqttEvent.Reconnecting -> println("reconnecting in ${event.delay}")
        else -> {} // new event types may be added in minor releases
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
Right before each wait, a `MqttEvent.Reconnecting(cause, delay)` is emitted (`cause` is null
for a normal disconnect); it is only ever emitted when `reconnect` is set.

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

`GatewayError`/`Upstream` messages can contain the target URL or upstream error text; don't send
them verbatim to crash reporters.

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
