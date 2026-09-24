# Athena-HttpMqttGateway

[![CI](https://github.com/Gekkotron/Athena-HttpMqttGateway/actions/workflows/ci.yml/badge.svg?branch=master)](https://github.com/Gekkotron/Athena-HttpMqttGateway/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/Gekkotron/Athena-HttpMqttGateway/badges/coverage.json)](https://github.com/Gekkotron/Athena-HttpMqttGateway/actions/workflows/ci.yml)

An end-to-end encrypted gateway for HTTP APIs and MQTT communications, using AES-GCM. Ship home-automation traffic over the public internet through a single encrypted tunnel — no per-service certificates, no exposed brokers.

## Features

- **AES-GCM Encryption**: Military-grade end-to-end encryption for all requests and responses
- **Multi-Service Support**: Unified gateway for both generic HTTP APIs and MQTT broker communications
- **MQTT Publish & Subscribe**: Full MQTT support with encrypted publish and SSE-based subscription streaming
- **Generic HTTP Proxy**: Forward any HTTP request with encrypted payload
- **Automatic Key Management**: Secure secret key generation on first launch
- **Replay Attack Protection**: Timestamp validation with configurable time windows
- **Docker Ready**: Complete containerization with Docker Compose support
- **Tailscale Funnel Integration**: Secure public internet exposure without port forwarding
- **Production Ready**: Comprehensive error handling, logging, and health checks

## Architecture

```
┌─────────────┐    Encrypted     ┌─────────────────────┐    Unencrypted    ┌──────────────┐
│   Client    │ ───────────────> │  Athena-HttpMqtt-   │ ────────────────> │  HTTP APIs   │
│ (Your App)  │ <─────────────── │      Gateway        │ <──────────────── │  (Any Server)│
└─────────────┘    AES-GCM       └─────────────────────┘                    └──────────────┘
                                            │
                                            │ Unencrypted
                                            ↓
                                    ┌─────────────┐
                                    │ MQTT Broker │
                                    └─────────────┘
```

The gateway acts as a secure proxy, encrypting/decrypting traffic between your client applications and backend services (HTTP APIs and MQTT).

## Quick Start

### Prerequisites

- Python 3.9+ or Docker
- (Optional) Tailscale for secure internet exposure

### Installation

#### Option 1: Docker (Recommended for Production)

1. Clone the repository:
```bash
git clone <repository-url>
cd FullEndToEndEncryption
```

2. Configure environment (optional):
```bash
cp .env.example .env
# Edit .env to customize settings
```

3. Start with Docker Compose:
```bash
docker compose up -d
```

The server will start on port 10000 and automatically generate a secret key in `data/secret_key.txt`.

#### Option 2: Direct Python (Development)

1. Create and activate a virtual environment:
```bash
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Run the server:
```bash
python -m server
```

### Secret Key Setup

On first launch, the server automatically generates a 256-bit AES secret key with full access (scope `*`) and displays it:

```
================================================================================
NEW SECRET KEY GENERATED!
================================================================================
Secret key: 3616864d6ed4f70b8a774e17c7435b411d8bc5714fddaf88e3850104a9d88ccd
Saved to: secret_key.txt
Scope: * (full access to every port)
================================================================================
Please update your client with this secret key.
================================================================================
```

**Important**: Save this key securely and configure your clients with it.

#### Multiple secrets with per-endpoint scopes

`secret_key.txt` is a **line-based** list of secrets. Each line is
`<64-hex-chars>[:<scope>]`, where the scope selects which endpoint
families the secret may reach:

| Port | Endpoints it unlocks             |
|------|----------------------------------|
| `80` | `/gateway` (HTTP forwarding)     |
| `1883` | `/mqtt/publish`, `/mqtt/subscribe` |

Scope syntax:

- `*` — full access to every port and destination (auto-generation writes this).
- Comma-separated port list — e.g. `80` (HTTP only), `80,1883` (both).
- Optional `@<destinations>` suffix restricts what target host each key
  may reach. Destinations are a comma-separated list of **IPs or CIDR
  ranges** (hostnames are rejected at parse time to keep the check
  DNS-independent):
  - `80@192.168.1.50` — HTTP forwarding, only to that IP.
  - `1883@192.168.1.0/24` — MQTT broker must be inside the /24.
  - `*@192.168.1.0/24` — full port access, LAN only.
  - Missing `@…` (or `@*`) means any destination.
- **No colon at all** — a bare hex line is treated as `*`, so legacy
  single-key files keep working unchanged.

Destination enforcement:

- `/gateway` extracts the target host from the payload's `url` (via
  `urlparse`) or `host` field.
- `/mqtt/publish` and `/mqtt/subscribe` use `broker_host` from the
  payload, or `MQTT_BROKER_HOST` from config if the client omits it.
- The client-sent value must parse as an IP; a hostname sent to an
  IP-scoped secret is refused with an encrypted 403.

Blank lines and `#` comments are ignored. Example:

```
# Full-access key for your admin client (auto-generated).
3616864d6ed4f70b8a774e17c7435b411d8bc5714fddaf88e3850104a9d88ccd:*

# HTTP-only key you can hand to a web-only service.
9d3a0e5cb1f0f5a5e3d4c1b6a7089d1e2f3a4b5c6d7e8f90112233445566778899:80

# Key that can do BOTH HTTP and MQTT (multiple ports, no destination filter).
2222222222222222222222222222222222222222222222222222222222222222:80,1883

# Same as above, but pinned to one target host (works for HTTP and MQTT
# because it's the same host for both).
3333333333333333333333333333333333333333333333333333333333333333:80,1883@192.168.1.91

# Multiple destinations on one line -- port(s) may reach any of them.
# Here: HTTP forwarding is allowed to two Jeedom boxes.
4444444444444444444444444444444444444444444444444444444444444444:80@192.168.1.50,192.168.1.51

# MQTT-only key for a device, LAN broker only.
aabb00112233445566778899aabbccddeeff00112233445566778899aabbccdd:1883@192.168.1.91

# Full access, but LAN only.
1122334455667788112233445566778811223344556677881122334455667788:*@192.168.1.0/24

# --- Per-port destination (different targets per port) ---
# The single-line syntax "80,1883@ip1,ip2" allows BOTH ports to reach BOTH
# ips. If you want port A -> host X and port B -> host Y under ONE key,
# split them across two lines reusing the SAME hex key. Lines sharing a
# key are merged into a single secret with multiple (ports, destinations)
# rules; a request is permitted when ANY rule matches its (port, host).
5555555555555555555555555555555555555555555555555555555555555555:80@192.168.1.50
5555555555555555555555555555555555555555555555555555555555555555:1883@192.168.1.91
```

The gateway trial-decrypts each incoming request against every configured
key. On a match, it enforces the key's scope against the endpoint being
called. Requests whose ciphertext no key can authenticate are rejected
with an **unencrypted HTTP 401** (the server cannot encrypt a response
without knowing which key the client holds).

#### Adding a new scoped secret

`python -m server.add_secret` generates a fresh random 256-bit key and
appends it to the secret-key file. Use it to hand out keys per service
without editing the file by hand:

```bash
# HTTP-only key
python -m server.add_secret --scope 80

# MQTT-only key with a label above it
python -m server.add_secret --scope 1883 --comment "device: living-room"

# HTTP-only key locked to one target IP
python -m server.add_secret --scope 80 --dest 192.168.1.50

# Full-access key restricted to the LAN
python -m server.add_secret --scope '*' --dest 192.168.1.0/24

# Embedded destination form is also accepted
python -m server.add_secret --scope '80@192.168.1.50'
```

The tool prints the new hex secret so you can copy it into a client.
Existing keys are left untouched; the file is validated before the append
so a corrupted line stops the tool before it writes anything.

## Configuration

### Environment Variables

You can configure the gateway using environment variables. Create a `.env` file in the project root:

```bash
cp .env.example .env
# Edit .env with your settings
```

| Variable | Default | Description |
|----------|---------|-------------|
| `SECRET_KEY_FILE` | `data/secret_key.txt` | Path to the secret key file |
| `MAX_AGE_SECONDS` | `60` | Maximum age for request timestamps (replay protection) |
| `HOST` | `0.0.0.0` | Server bind address |
| `PORT` | `10000` | Server port |
| `MQTT_BROKER_HOST` | `192.168.1.91` | Default MQTT broker hostname |
| `MQTT_BROKER_PORT` | `1883` | Default MQTT broker port |

### Docker Volumes

The Docker setup uses a persistent volume for the secret key:
- `./data:/app/data` - Stores the secret key across container restarts

## Usage

### Client Implementation

The project includes ready-to-use Python client classes for HTTP and MQTT:

#### HTTP Request Example

```python
from test_client.client_http_test import EncryptedClient

# Load secret key from file
with open('data/secret_key.txt', 'r') as f:
    # Line-based file: each entry is `<hex>[:<scope>]` -- take the hex part.
    SECRET_KEY = f.readline().strip().split(':', 1)[0]

# Initialize client
client = EncryptedClient(
    gateway_url="http://localhost:10000",
    secret_key=SECRET_KEY
)

# Make encrypted HTTP request
response = client.send_request(
    url="https://api.example.com/endpoint",
    method="POST",
    body={"key": "value"},
    headers={"Content-Type": "application/json"}
)

print(f"Status: {response['status']}")
print(f"Body: {response['body']}")
```

#### Jeedom JSON-RPC Example

Use the gateway to communicate with a Jeedom home automation server via its JSON-RPC API:

```python
from test_client.client_http_test import EncryptedHttpClient

# Load secret key
with open('data/secret_key.txt', 'r') as f:
    # Line-based file: each entry is `<hex>[:<scope>]` -- take the hex part.
    SECRET_KEY = f.readline().strip().split(':', 1)[0]

client = EncryptedHttpClient(
    gateway_url="https://yourdevice.tailnet-name.ts.net",
    secret_key=SECRET_KEY
)

# Get Jeedom server datetime
response = client.send_request(
    url="http://jeedom-host/core/api/jeeApi.php",
    method="POST",
    headers={"Content-Type": "application/json"},
    body={
        "jsonrpc": "2.0",
        "id": 1,
        "method": "datetime",
        "params": {
            "apikey": "your-jeedom-api-key"
        }
    }
)

print(f"Status: {response['status']}")
print(f"Jeedom datetime: {response['body']['result']}")

# Get all objects with full details
response = client.send_request(
    url="http://jeedom-host/core/api/jeeApi.php",
    method="POST",
    headers={"Content-Type": "application/json"},
    body={
        "jsonrpc": "2.0",
        "id": 1,
        "method": "jeeObject::full",
        "params": {
            "apikey": "your-jeedom-api-key"
        }
    }
)

for obj in response['body']['result']:
    print(f"Object: {obj['name']} (id={obj['id']})")

# Execute a command (e.g., turn on a light)
response = client.send_request(
    url="http://jeedom-host/core/api/jeeApi.php",
    method="POST",
    headers={"Content-Type": "application/json"},
    body={
        "jsonrpc": "2.0",
        "id": 1,
        "method": "cmd::execCmd",
        "params": {
            "apikey": "your-jeedom-api-key",
            "id": 42,  # Command ID
            "options": {}
        }
    }
)

print(f"Command result: {response['body']}")
```

#### MQTT Publish Example

```python
from test_client.client_mqtt_test import EncryptedClient

# Initialize client
client = EncryptedClient(
    gateway_url="http://localhost:10000",
    secret_key=SECRET_KEY
)

# Publish encrypted message
response = client.send_mqtt(
    topic="home/temperature",
    message="22.5",
    qos=1,
    retain=True
)

print(f"Status: {response['status']}")
print(f"Body: {response['body']}")
```

#### MQTT Subscribe (SSE) Example

Subscribe to MQTT topics and receive messages in real-time via Server-Sent Events:

```python
from test_client.client_mqtt_sse_test import subscribe_to_mqtt_topic

# Subscribe to a topic
subscribe_to_mqtt_topic(
    server_url="http://localhost:10000",
    topic="home/sensors/#",  # Supports MQTT wildcards
    qos=1
)

# With custom broker settings
subscribe_to_mqtt_topic(
    server_url="http://localhost:10000",
    topic="home/temperature",
    broker_host="192.168.1.100",
    broker_port=1883,
    username="mqtt_user",
    password="mqtt_password",
    qos=1
)
```

The SSE stream will receive encrypted messages with the following types:
- **connected**: Successfully subscribed to the topic
- **message**: MQTT message received (includes topic, payload, qos, retain)
- **error**: An error occurred
- **disconnected**: Connection closed

#### Android Example (Kotlin)

For Android applications, you can implement the encrypted client using Kotlin:

```kotlin
import android.util.Base64
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import org.json.JSONObject
import java.security.SecureRandom
import javax.crypto.Cipher
import javax.crypto.spec.GCMParameterSpec
import javax.crypto.spec.SecretKeySpec

class EncryptedGatewayClient(
    private val gatewayUrl: String,
    private val secretKey: String
) {
    private val client = OkHttpClient()
    private val keyBytes = hexStringToByteArray(secretKey)

    // Convert hex string to byte array
    private fun hexStringToByteArray(s: String): ByteArray {
        val len = s.length
        val data = ByteArray(len / 2)
        var i = 0
        while (i < len) {
            data[i / 2] = ((Character.digit(s[i], 16) shl 4) +
                          Character.digit(s[i + 1], 16)).toByte()
            i += 2
        }
        return data
    }

    // Encrypt data using AES-GCM
    private fun encrypt(data: JSONObject): ByteArray {
        val nonce = ByteArray(12)
        SecureRandom().nextBytes(nonce)

        val cipher = Cipher.getInstance("AES/GCM/NoPadding")
        val keySpec = SecretKeySpec(keyBytes, "AES")
        val gcmSpec = GCMParameterSpec(128, nonce)
        cipher.init(Cipher.ENCRYPT_MODE, keySpec, gcmSpec)

        val plaintext = data.toString().toByteArray(Charsets.UTF_8)
        val ciphertext = cipher.doFinal(plaintext)

        // Return nonce + ciphertext
        return nonce + ciphertext
    }

    // Decrypt data using AES-GCM
    private fun decrypt(data: ByteArray): JSONObject {
        val nonce = data.sliceArray(0..11)
        val ciphertext = data.sliceArray(12 until data.size)

        val cipher = Cipher.getInstance("AES/GCM/NoPadding")
        val keySpec = SecretKeySpec(keyBytes, "AES")
        val gcmSpec = GCMParameterSpec(128, nonce)
        cipher.init(Cipher.DECRYPT_MODE, keySpec, gcmSpec)

        val plaintext = cipher.doFinal(ciphertext)
        return JSONObject(String(plaintext, Charsets.UTF_8))
    }

    // Send encrypted HTTP request
    suspend fun sendHttpRequest(
        url: String,
        method: String = "POST",
        body: JSONObject? = null,
        headers: JSONObject? = null
    ): JSONObject = withContext(Dispatchers.IO) {
        val payload = JSONObject().apply {
            put("url", url)
            put("method", method)
            put("timestamp", System.currentTimeMillis() / 1000)
            if (body != null) {
                put("body", body)
            }
            if (headers != null) {
                put("headers", headers)
            }
        }

        val encrypted = encrypt(payload)
        val encoded = Base64.encodeToString(encrypted, Base64.NO_WRAP)

        val requestBody = encoded.toRequestBody(
            "application/octet-stream".toMediaType()
        )

        val request = Request.Builder()
            .url("$gatewayUrl/gateway")
            .post(requestBody)
            .build()

        val response = client.newCall(request).execute()
        val responseData = response.body?.bytes()
            ?: throw Exception("Empty response")

        val decoded = Base64.decode(responseData, Base64.NO_WRAP)
        decrypt(decoded)
    }

    // Send encrypted MQTT publish request
    suspend fun sendMqttPublish(
        topic: String,
        message: String,
        qos: Int = 0,
        retain: Boolean = false,
        brokerHost: String? = null,
        brokerPort: Int? = null,
        username: String? = null,
        password: String? = null
    ): JSONObject = withContext(Dispatchers.IO) {
        val payload = JSONObject().apply {
            put("topic", topic)
            put("message", message)
            put("qos", qos)
            put("retain", retain)
            put("timestamp", System.currentTimeMillis() / 1000)
            if (brokerHost != null) put("broker_host", brokerHost)
            if (brokerPort != null) put("broker_port", brokerPort)
            if (username != null) put("username", username)
            if (password != null) put("password", password)
        }

        val encrypted = encrypt(payload)
        val encoded = Base64.encodeToString(encrypted, Base64.NO_WRAP)

        val requestBody = encoded.toRequestBody(
            "application/octet-stream".toMediaType()
        )

        val request = Request.Builder()
            .url("$gatewayUrl/mqtt/publish")
            .post(requestBody)
            .build()

        val response = client.newCall(request).execute()
        val responseData = response.body?.bytes()
            ?: throw Exception("Empty response")

        val decoded = Base64.decode(responseData, Base64.NO_WRAP)
        decrypt(decoded)
    }
}

// Usage example in an Activity or ViewModel
class MainActivity : AppCompatActivity() {
    private lateinit var client: EncryptedGatewayClient

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)

        // Initialize client
        client = EncryptedGatewayClient(
            gatewayUrl = "https://yourdevice.tailnet-name.ts.net",
            secretKey = "your-secret-key-here"
        )

        // Make encrypted requests
        lifecycleScope.launch {
            try {
                // HTTP request example
                val httpResponse = client.sendHttpRequest(
                    url = "https://api.example.com/data",
                    method = "POST",
                    body = JSONObject().apply {
                        put("key", "value")
                    }
                )

                Log.d("Gateway", "Status: ${httpResponse.getInt("status")}")
                Log.d("Gateway", "Body: ${httpResponse.get("body")}")

                // MQTT publish request
                val mqttResponse = client.sendMqttPublish(
                    topic = "home/temperature",
                    message = "22.5",
                    qos = 1,
                    retain = true
                )

                Log.d("Gateway", "MQTT Status: ${mqttResponse.getInt("status")}")

            } catch (e: Exception) {
                Log.e("Gateway", "Error: ${e.message}", e)
            }
        }
    }
}
```

**Android Dependencies (build.gradle.kts):**

```kotlin
dependencies {
    implementation("com.squareup.okhttp3:okhttp:4.12.0")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-android:1.7.3")
    // JSON is included in Android SDK
}
```

**Required Permissions (AndroidManifest.xml):**

```xml
<uses-permission android:name="android.permission.INTERNET" />
```

**Security Best Practices for Android:**

1. **Store the secret key securely** using Android's EncryptedSharedPreferences:

```kotlin
import androidx.security.crypto.EncryptedSharedPreferences
import androidx.security.crypto.MasterKey

fun saveSecretKey(context: Context, secretKey: String) {
    val masterKey = MasterKey.Builder(context)
        .setKeyScheme(MasterKey.KeyScheme.AES256_GCM)
        .build()

    val sharedPreferences = EncryptedSharedPreferences.create(
        context,
        "secure_prefs",
        masterKey,
        EncryptedSharedPreferences.PrefKeyEncryptionScheme.AES256_SIV,
        EncryptedSharedPreferences.PrefValueEncryptionScheme.AES256_GCM
    )

    sharedPreferences.edit().putString("secret_key", secretKey).apply()
}

fun getSecretKey(context: Context): String? {
    val masterKey = MasterKey.Builder(context)
        .setKeyScheme(MasterKey.KeyScheme.AES256_GCM)
        .build()

    val sharedPreferences = EncryptedSharedPreferences.create(
        context,
        "secure_prefs",
        masterKey,
        EncryptedSharedPreferences.PrefKeyEncryptionScheme.AES256_SIV,
        EncryptedSharedPreferences.PrefValueEncryptionScheme.AES256_GCM
    )

    return sharedPreferences.getString("secret_key", null)
}
```

2. **Add ProGuard/R8 rules** to protect sensitive code (proguard-rules.pro):

```proguard
# Keep encryption classes
-keep class javax.crypto.** { *; }
-keep class javax.crypto.spec.** { *; }
-keep class your.package.EncryptedGatewayClient { *; }
```

3. **Use certificate pinning** for production to prevent man-in-the-middle attacks:

```kotlin
import okhttp3.CertificatePinner

val certificatePinner = CertificatePinner.Builder()
    .add("yourdevice.tailnet-name.ts.net", "sha256/AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")
    .build()

private val client = OkHttpClient.Builder()
    .certificatePinner(certificatePinner)
    .build()
```

### Request Format

All requests must include a `timestamp` field for replay protection:

```json
{
    "timestamp": 1234567890,
    // Endpoint-specific fields...
}
```

Different endpoints require different fields (see API Endpoints section for details).

#### MQTT Publish Request Fields

- `topic`: MQTT topic to publish to
- `message`: Message payload
- `broker_host`: (Optional) MQTT broker hostname
- `broker_port`: (Optional) MQTT broker port
- `username`: (Optional) MQTT username
- `password`: (Optional) MQTT password
- `qos`: (Optional) Quality of Service (0-2)
- `retain`: (Optional) Retain flag
- `timestamp`: Current Unix timestamp

#### MQTT Subscribe (SSE) Request Fields

Used with the `/mqtt/subscribe` endpoint for real-time subscriptions:

- `topics`: List of MQTT topics to subscribe to on one stream (supports wildcards: `+` for single level, `#` for multi-level)
- `topic`: Single MQTT topic (legacy form; may be combined with `topics`). At least one of `topic` / `topics` is required
- `broker_host`: (Optional) MQTT broker hostname
- `broker_port`: (Optional) MQTT broker port
- `username`: (Optional) MQTT username
- `password`: (Optional) MQTT password
- `qos`: (Optional) Quality of Service (0-2), applied to every topic
- `timestamp`: Current Unix timestamp (required for replay protection)

**Example Subscribe Payload:**
```json
{
    "topics": ["home/sensors/#", "home/lights/+/state"],
    "broker_host": "192.168.1.91",
    "broker_port": 1883,
    "qos": 1,
    "timestamp": 1234567890
}
```

**SSE Response Messages:**

Each SSE event contains an encrypted payload with one of these message types:

1. **Connected Event:**
```json
{
    "type": "connected",
    "topic": "home/sensors/#",
    "topics": ["home/sensors/#", "home/lights/+/state"],
    "message": "Successfully connected and subscribed to home/sensors/#, home/lights/+/state"
}
```

2. **Message Event:**
```json
{
    "type": "message",
    "topic": "home/sensors/temperature",
    "payload": "22.5",
    "qos": 1,
    "retain": false,
    "timestamp": 1234567890
}
```

3. **Error Event:**
```json
{
    "type": "error",
    "message": "Connection failed with code 5",
    "timestamp": 1234567890
}
```

4. **Disconnected Event:**
```json
{
    "type": "disconnected",
    "message": "Disconnected from broker (code 0)",
    "timestamp": 1234567890
}
```

All messages are encrypted using AES-GCM and base64-encoded before being sent via SSE.

### Response Format

All responses are encrypted and contain:

```json
{
    "status": 200,
    "body": "...",
    "timestamp": 1234567890
}
```

## Testing

### Unit Tests

Offline tests (no broker, no network) run on every push via GitHub Actions:

```bash
pip install -r requirements-dev.txt
python -m pytest --cov=server --cov-report=term-missing
```

### Integration Test Clients

These talk to a running gateway. Point them at it with `GATEWAY_URL`
(default `http://localhost:10000`), e.g. from your git-ignored `.env`.

### Run All Tests

```bash
python test_client/client_all_services_test.py
```

### Run Individual Tests

```bash
# HTTP gateway tests
python test_client/client_http_test.py

# MQTT tests (4 test cases)
python test_client/client_mqtt_test.py
```

### Test Coverage

**HTTP Gateway Tests:**
- Valid timestamp requests
- Expired timestamp rejection
- Future timestamp rejection
- Wrong secret key handling
- Missing required fields
- Multiple rapid requests (stress test)
- Different HTTP methods (GET, POST, PUT, DELETE)

**MQTT Tests:**
- Basic message publishing
- QoS and retain flags
- Custom broker configuration
- Error handling for missing fields

## Health Checks

Two independent health checks help diagnose flaky home-automation connections. Run each on its own, or both together — the exit code makes them cron/monitoring friendly (`0` OK, `1` failure, `2` configuration error).

```bash
# Both checks (default)
python -m test_client.health_check

# HTTP gateway only: /health ping + encrypted round-trip
python -m test_client.health_check --gateway

# Tailscale only: daemon + funnel + remote HTTPS probe
python -m test_client.health_check --tailscale

# Auto-remediate: restart whatever failed, then re-check
python -m test_client.health_check --restart

# Quiet mode (exit code only) — cron-friendly
python -m test_client.health_check --all -q --restart
```

**Auto-restart (`--restart`):** on a *hard failure* (exit 1), the failing component is restarted, then re-checked with **exponential backoff** until it recovers or the attempt budget is exhausted. Configuration errors (exit 2) never trigger a restart — fix the config first.

- Gateway restart defaults to `docker compose restart gateway` (matching `docker-compose.yml`). Override with `GATEWAY_RESTART_CMD` if you run the app another way.
- Tailscale restart defaults to `./run_tailscale.sh` — the same script used to bring the funnel up manually. The script uses `sudo` internally, so passwordless sudo is required for headless/cron use. Override with `TAILSCALE_RESTART_CMD` if you want something else.

Post-restart backoff (delays are cumulative, loop stops on the first PASS):

| Variable | Purpose | Default |
|---|---|---|
| `RESTART_BACKOFF_INITIAL` | Seconds before the first re-check | `2` |
| `RESTART_BACKOFF_FACTOR` | Multiplier applied between attempts | `2` |
| `RESTART_BACKOFF_MAX_ATTEMPTS` | Maximum re-check attempts (`0` = retry forever) | `5` |
| `RESTART_BACKOFF_MAX_DELAY` | Per-attempt delay cap in seconds | `300` |

With defaults, delays are 2s, 4s, 8s, 16s, 32s (max ~62s of waiting).
Set `RESTART_BACKOFF_MAX_ATTEMPTS=0` to retry forever — the delay grows exponentially, plateaus at `RESTART_BACKOFF_MAX_DELAY` (5min by default), and the loop only exits when the check passes. Pair this with a systemd/cron *timer* rather than a supervisor loop if you want to be able to kill it easily.

### MQTT state topic

After each check completes, the orchestrator publishes the current state as a JSON message to the configured MQTT broker. The message is **retained** by default so a subscriber (Jeedom, Home Assistant, …) that connects between runs immediately sees the last known state — no need to wait for the next timer fire. Publishing is a side channel; if the broker is unreachable, a warning goes to stderr but the exit code is not affected.

**Semantics:** state, not event. `status` cycles through `ok` / `fail` / `config_error` on every run. Downstream automations should compare the payload's `status` field against their expected value rather than treating any message as an alert. If you preferred the old "silent while healthy" behaviour, set `HEALTH_ALERT_PUBLISH_ON_SUCCESS=false` — but also set `HEALTH_ALERT_MQTT_RETAIN=false`, otherwise a resolved failure sticks around as a retained message forever.

**Topic layout:** `<HEALTH_ALERT_MQTT_TOPIC_PREFIX>/<component>`

Default topics (matching the docker-compose `container_name: encryption-gateway`):
- `encryption-gateway/health/gateway`
- `encryption-gateway/health/tailscale`

Override `HEALTH_ALERT_MQTT_TOPIC_PREFIX` if you'd rather group these under something else (e.g. per-house or per-room).

**Payload (JSON):**
```json
{
  "component": "gateway",
  "status": "ok",             // "ok" | "fail" | "config_error"
  "exit_code": 0,
  "restart_attempted": false,
  "timestamp": 1723456789,
  "host": "raspberrypi"
}
```

**Env vars:**

| Variable | Purpose | Default |
|---|---|---|
| `MQTT_BROKER_HOST` | Broker hostname (required for publishing) | *(unset — disabled)* |
| `MQTT_BROKER_PORT` | Broker port | `1883` |
| `MQTT_USERNAME` / `MQTT_PASSWORD` | Optional broker credentials | *(unset)* |
| `HEALTH_ALERT_ENABLED` | `auto` (enable if broker set), `true`, or `false` | `auto` |
| `HEALTH_ALERT_PUBLISH_ON_SUCCESS` | Publish on PASS too (state topic) or only on FAIL (event) | `true` |
| `HEALTH_ALERT_MQTT_TOPIC_PREFIX` | Topic prefix | `encryption-gateway/health` |
| `HEALTH_ALERT_MQTT_QOS` | QoS 0/1/2 | `1` |
| `HEALTH_ALERT_MQTT_RETAIN` | Retain flag — leave on for state semantics | `true` |
| `HEALTH_ALERT_MQTT_TIMEOUT` | Connect timeout, seconds | `5` |

Pass `--no-alert` on the command line to skip publishing for a single invocation (useful when running the check interactively during debugging).

**Gateway check** (`test_client/health_check_gateway.py`) runs:
1. `GET /health` — proves Flask is up.
2. Encrypted round-trip through `/gateway` to `HEALTH_CHECK_URL` (default `https://httpbin.org/get`) — proves the crypto path is intact end-to-end.

**Tailscale check** (`test_client/health_check_tailscale.py`) runs:
1. `tailscale status --json` — daemon is running and this node is online.
2. `tailscale funnel status` — funnel is bound to the expected port.
3. HTTPS `GET /health` on `TAILSCALE_FUNNEL_URL` — the public URL is reachable.

Additional environment variables (see `.env.example`):

| Variable | Purpose | Default |
|---|---|---|
| `HEALTH_CHECK_URL` | Target URL for the encrypted round-trip | `https://httpbin.org/get` |
| `HEALTH_TIMEOUT` | Per-request timeout, seconds | `10` |
| `TAILSCALE_FUNNEL_URL` | Public funnel URL to probe remotely | *(unset — remote probe fails)* |
| `TAILSCALE_FUNNEL_PORT` | Expected port bound to the funnel | `10000` |
| `TAILSCALE_BIN` | tailscale CLI binary path | `tailscale` |
| `TAILSCALE_CHECK_LOCAL` | `auto`/`true`/`false` — run local daemon+funnel checks | `auto` |
| `GATEWAY_RESTART_CMD` | Command run by `--restart` for the gateway | `docker compose restart gateway` |
| `TAILSCALE_RESTART_CMD` | Command run by `--restart` for the funnel | `./run_tailscale.sh` |

### Running the health check automatically at startup (systemd)

Ships with a `.service` + `.timer` pair under `scripts/systemd/`. The timer fires once ~1 minute after boot and then every 5 minutes; the service runs `python -m test_client.health_check --all --restart` and exits (single-shot per fire). Compared to a supervisor loop, this pattern:

- Uses the system journal for logs (`journalctl -u encryption-gateway-health -f`).
- Bounds each run with `TimeoutStartSec=15min`, so a hung network call can't jam future firings.
- Can be started/stopped cleanly with `systemctl` — no PID files.

**Install (one command; rerun after moving the repo or changing the venv):**

```bash
sudo scripts/install-systemd.sh
```

The installer picks up the current directory as `REPO_DIR`, the invoking user (`$SUDO_USER`) as the service user, and `./.venv/bin/python` if you use a venv (falling back to system `python3`). Override any of these with env vars, e.g. `sudo REPO_DIR=/opt/enc-gw PYTHON_BIN=/usr/local/bin/python3.11 scripts/install-systemd.sh`.

**Common ops:**

```bash
systemctl status encryption-gateway-health.timer      # is the timer armed?
systemctl list-timers | grep encryption-gateway       # when's the next fire?
journalctl -u encryption-gateway-health.service -f    # follow live logs
systemctl start encryption-gateway-health.service     # run one check immediately
systemctl disable --now encryption-gateway-health.timer   # stop the periodic runs
```

**Tuning the interval:** edit `OnUnitActiveSec=` in `scripts/systemd/encryption-gateway-health.timer` and rerun the installer.

### Cron alternative (non-systemd hosts)

If you're not on systemd, a plain crontab entry works fine:

```cron
@reboot   sleep 60 && cd /opt/enc-gw && /opt/enc-gw/.venv/bin/python -m test_client.health_check --all --restart >> /var/log/enc-gw-health.log 2>&1
*/5 * * * * cd /opt/enc-gw && /opt/enc-gw/.venv/bin/python -m test_client.health_check --all --restart >> /var/log/enc-gw-health.log 2>&1
```

Note that cron doesn't provide the `TimeoutStartSec` safety net — a hung check can pile up if the interval is short. Prefer systemd where available.

## Tailscale Funnel Deployment

Expose your gateway securely over the internet using Tailscale Funnel:

1. Install and authenticate Tailscale on your machine
2. Enable Funnel for your account
3. Start the server
4. Run the Tailscale script:

```bash
chmod +x run_tailscale.sh
./run_tailscale.sh
```

Your gateway will be accessible via a public HTTPS URL like `https://yourdevice.tailnet-name.ts.net`.

## Security Features

### Encryption

- **Algorithm**: AES-GCM with 256-bit keys
- **Nonce**: 12-byte random nonce per message
- **Authentication**: Built-in authenticated encryption prevents tampering
- **Key Management**: Automatic secure key generation using OS-level randomness

### Replay Attack Protection

The gateway validates timestamps to prevent replay attacks:
- Requests older than `MAX_AGE_SECONDS` are rejected
- Future timestamps are also rejected
- Default window: 60 seconds (configurable)
- Each request's AES-GCM nonce is remembered for the length of the window, so an identical request replayed inside the window is rejected with `Request replayed`
- Applies to all three encrypted endpoints: `/gateway`, `/mqtt/publish` and `/mqtt/subscribe`

### Best Practices

1. **Store the secret key securely**: Never commit it to version control
2. **Use HTTPS**: Deploy with Tailscale Funnel or behind a reverse proxy with TLS
3. **Rotate keys periodically**: Generate new keys and update clients
4. **Monitor logs**: Watch for suspicious activity or repeated failed decryption attempts
5. **Network isolation**: Run the gateway in a separate network segment if possible

## Project Structure

```
FullEndToEndEncryption/
├── server/
│   ├── __init__.py
│   ├── __main__.py          # Entry point
│   ├── app.py               # Flask application factory
│   ├── config.py            # Configuration management
│   ├── crypto.py            # AES-GCM encryption/decryption
│   ├── gateway.py           # Request routing and handling
│   ├── key_manager.py       # Secret key generation
│   └── services/
│       ├── http_service.py        # Generic HTTP proxy handler
│       ├── mqtt_service.py        # MQTT publish handler
│       └── mqtt_sse_service.py    # MQTT subscribe via SSE handler
├── test_client/
│   ├── client_http_test.py        # HTTP gateway client and tests
│   ├── client_mqtt_test.py        # MQTT publish client and tests
│   ├── client_mqtt_sse_test.py    # MQTT SSE subscribe client
│   └── client_all_services_test.py  # Test runner
├── docker-compose.yml       # Docker Compose configuration
├── Dockerfile               # Container image definition
├── requirements.txt         # Python dependencies
├── run_tailscale.sh        # Tailscale Funnel helper script
└── README.md
```

## API Endpoints

### POST /gateway

Gateway endpoint for encrypted HTTP requests to any API.

- **Content-Type**: `application/octet-stream`
- **Body**: Base64-encoded encrypted payload
- **Response**: Base64-encoded encrypted response

**Payload Fields:**
- `url`: Target HTTP server URL (required if `host` not provided)
- `host`: Target server host (required if `url` not provided, e.g., "http://example.com:8080")
- `endpoint`: API endpoint path (optional, defaults to "/", e.g., "/api/v1/data")
- `method`: HTTP method (GET, POST, PUT, DELETE, etc.), defaults to "POST"
- `body`: Request body (dict for JSON or string for raw data)
- `headers`: (Optional) HTTP headers dict, defaults to `{"Content-Type": "application/json"}`
- `timeout`: (Optional) Request timeout in seconds, defaults to 30
- `timestamp`: Current Unix timestamp (required for replay protection)

**Usage:**
This endpoint acts as a generic encrypted HTTP proxy. You can forward any HTTP request through it with end-to-end encryption. The gateway will decrypt your request, forward it to the target URL, and return the encrypted response.

You can specify the target in two ways:
1. **Complete URL**: Use the `url` parameter with the full URL
2. **Host + Endpoint**: Use `host` (e.g., "http://api.example.com") and `endpoint` (e.g., "/api/data") - they will be combined into a complete URL

### POST /mqtt/publish

Encrypted MQTT publish endpoint for publishing messages to MQTT brokers.

- **Content-Type**: `application/octet-stream`
- **Body**: Base64-encoded encrypted payload with MQTT publish details
- **Response**: Base64-encoded encrypted response

**Payload Fields:**
- `topic`: MQTT topic to publish to (required)
- `message`: Message payload to publish (required)
- `qos`: (Optional) Quality of Service (0-2), defaults to 0
- `retain`: (Optional) Retain flag, defaults to false
- `broker_host`: (Optional) MQTT broker hostname
- `broker_port`: (Optional) MQTT broker port
- `username`: (Optional) MQTT username
- `password`: (Optional) MQTT password
- `timestamp`: Current Unix timestamp (required for replay protection)

**Usage:**
This endpoint allows you to publish messages to MQTT topics through the encrypted gateway. Messages are published once and the response confirms success or failure.

### POST /mqtt/subscribe

Server-Sent Events endpoint for subscribing to MQTT topics in real-time.

- **Content-Type**: `application/octet-stream`
- **Body**: Base64-encoded encrypted payload with subscription details
- **Response**: `text/event-stream` with encrypted SSE messages
- **Headers**:
  - `Cache-Control: no-cache`
  - `X-Accel-Buffering: no`
  - `Connection: keep-alive`

**Usage:**
This endpoint establishes a persistent HTTP connection and streams MQTT messages as they arrive. Each SSE event contains an encrypted message that must be decrypted by the client. The stream remains open until:
- An error occurs
- The MQTT connection is lost
- The client closes the connection

### GET /health

Health check endpoint.

- **Response**: `{"status": "ok", "version": "1.0.0"}`

## Dependencies

### Server Dependencies

- **Flask 3.0.0**: Web framework
- **cryptography 41.0.7**: AES-GCM encryption
- **requests 2.31.0**: HTTP client for proxying requests
- **paho-mqtt 1.6.1**: MQTT client library
- **python-dotenv 1.0.0**: Environment variable management

### Client Dependencies

- **sseclient-py 1.8.0**: SSE client support (for MQTT subscriptions)

## Development

### Running in Development Mode

```bash
# Activate virtual environment
source .venv/bin/activate

# Run with auto-reload
FLASK_ENV=development python -m server
```

### Building Docker Image

```bash
docker build -t encryption-gateway:latest .
```

### Code Organization

- **Modular Design**: Separate services for different protocols
- **Type Hints**: Full type annotations for better IDE support
- **Error Handling**: Comprehensive exception handling and logging
- **Clean Architecture**: Clear separation of concerns

## Troubleshooting

### Connection Refused

Ensure the server is running and accessible:
```bash
curl http://localhost:10000/health
```

### Decryption Failed

- Verify the secret key matches between client and server
- Check that the key file hasn't been corrupted
- Ensure timestamp is within the valid window

### MQTT Connection Failed

- Verify MQTT broker is running and accessible
- Check broker hostname and port configuration
- Validate MQTT credentials if authentication is enabled

### Docker Volume Permissions

If you encounter permission issues with the data volume:
```bash
sudo chown -R 1000:1000 ./data
```

## License

This project is provided as-is for use in securing smart home and IoT communications.

## Contributing

Contributions are welcome! Areas for improvement:

- Additional service handlers (Home Assistant, InfluxDB, etc.)
- Key rotation mechanism
- Multi-user support with per-user keys
- Rate limiting and DDoS protection
- Prometheus metrics endpoint
- Android SSE client implementation example

## Acknowledgments

Built with:
- Flask for the web framework
- Cryptography library for robust encryption
- Paho MQTT for MQTT support
- Docker for containerization

## Support

For issues, questions, or contributions, please open an issue on the project repository.
