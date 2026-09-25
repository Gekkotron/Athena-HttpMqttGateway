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
