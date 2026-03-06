from flask import Flask, jsonify
from app.core.config import get_config
from app.api.upload import upload_bp
from app.api.movies import movies_bp


def create_app(config=None) -> Flask:
    app = Flask(__name__)
    cfg = config or get_config()
    app.config.from_object(cfg)

    # Register blueprints
    app.register_blueprint(upload_bp)
    app.register_blueprint(movies_bp)

    # Health check
    @app.route("/health")
    def health():
        return jsonify({"status": "ok"}), 200

    # Global error handlers
    @app.errorhandler(404)
    def not_found(e):
        return jsonify({"error": "Not found"}), 404

    @app.errorhandler(405)
    def method_not_allowed(e):
        return jsonify({"error": "Method not allowed"}), 405

    @app.errorhandler(413)
    def too_large(e):
        return jsonify({"error": "File too large. Maximum allowed size is 2GB."}), 413

    @app.errorhandler(500)
    def internal_error(e):
        return jsonify({"error": "Internal server error"}), 500

    return app
