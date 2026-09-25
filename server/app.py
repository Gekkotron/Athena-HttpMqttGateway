"""Flask application factory and routes."""
import base64
import time
from flask import Flask, request, Response, stream_with_context

from .crypto import CryptoManager, NoKeyMatched
from .gateway import GatewayHandler
from .key_manager import load_or_generate_secrets, Secret
from .replay import ReplayGuard
from .services.mqtt_service import MQTTService
from .services.mqtt_sse_service import MQTTSSEService
from . import config


def _encrypted_error(crypto: CryptoManager, secret: Secret, status: int, message: str) -> Response:
    error_payload = {
        "status": status,
        "body": {"error": message},
        "timestamp": int(time.time()),
        "source": "gateway",
    }
    encrypted = base64.b64encode(crypto.encrypt(error_payload, secret))
    return Response(encrypted, mimetype="application/octet-stream")


def create_app() -> Flask:
    """Create and configure the Flask application."""
    app = Flask(__name__)

    # Load all configured secrets (or auto-generate one wildcard secret).
    secrets = load_or_generate_secrets(config.SECRET_KEY_FILE)

    crypto_manager = CryptoManager(secrets)
    replay_guard = ReplayGuard(config.MAX_AGE_SECONDS)
    gateway_handler = GatewayHandler(crypto_manager, replay_guard)
    mqtt_service = MQTTService(crypto_manager)
    mqtt_sse_service = MQTTSSEService(crypto_manager)

    @app.route("/gateway", methods=["POST"])
    def gateway():
        return gateway_handler.handle_request()

    @app.route("/health", methods=["GET"])
    def health():
        return {"status": "ok", "version": "1.2.0"}, 200

    @app.route("/mqtt/publish", methods=["POST"])
    def mqtt_publish():
        try:
            encrypted_request = base64.b64decode(request.data)
        except Exception:
            return Response("", status=400)

        try:
            payload, secret = crypto_manager.decrypt(encrypted_request)
        except NoKeyMatched:
            return Response("", status=401)
        except Exception:
            return Response("", status=400)

        if not secret.allows_port(config.MQTT_PORT):
            return _encrypted_error(crypto_manager, secret, 403, "port not allowed for this secret")

        replay_error = replay_guard.check(encrypted_request[:12], payload.get("timestamp"))
        if replay_error:
            return _encrypted_error(crypto_manager, secret, 403, replay_error)

        broker_host = payload.get("broker_host") or config.MQTT_BROKER_HOST
        if not secret.permits(config.MQTT_PORT, broker_host):
            return _encrypted_error(
                crypto_manager, secret, 403,
                "destination not allowed for this secret",
            )

        try:
            response = mqtt_service.handle_request(payload, secret)
        except Exception as e:
            return _encrypted_error(crypto_manager, secret, 500, str(e))

        return Response(base64.b64encode(response), mimetype="application/octet-stream")

    @app.route("/mqtt/subscribe", methods=["POST"])
    def mqtt_subscribe():
        try:
            encrypted_request = base64.b64decode(request.data)
        except Exception:
            return {"error": "malformed request"}, 400

        try:
            payload, secret = crypto_manager.decrypt(encrypted_request)
        except NoKeyMatched:
            return Response("", status=401)
        except Exception as e:
            return {"error": str(e)}, 400

        if not secret.allows_port(config.MQTT_PORT):
            # SSE stream not started -- return an encrypted-error frame via
            # the /mqtt/publish-style response so clients sharing the decrypt
            # path can read it.
            return _encrypted_error(crypto_manager, secret, 403, "port not allowed for this secret")

        replay_error = replay_guard.check(encrypted_request[:12], payload.get("timestamp"))
        if replay_error:
            return _encrypted_error(crypto_manager, secret, 403, replay_error)

        broker_host = payload.get("broker_host") or config.MQTT_BROKER_HOST
        if not secret.permits(config.MQTT_PORT, broker_host):
            return _encrypted_error(
                crypto_manager, secret, 403,
                "destination not allowed for this secret",
            )

        def generate():
            return mqtt_sse_service.subscribe_stream(payload, secret)

        return Response(
            stream_with_context(generate()),
            mimetype="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "X-Accel-Buffering": "no",
                "Connection": "keep-alive",
            },
        )

    return app


def run_server():
    """Run the Flask development server."""
    app = create_app()
    print(f"Starting server on {config.HOST}:{config.PORT}")
    app.run(host=config.HOST, port=config.PORT)


if __name__ == "__main__":
    run_server()
