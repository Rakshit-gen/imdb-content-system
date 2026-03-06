from flask import jsonify


def error_response(message: str, status: int = 400, details=None) -> tuple:
    body = {"error": message}
    if details:
        body["details"] = details
    return jsonify(body), status


def success_response(data: dict, status: int = 200) -> tuple:
    return jsonify(data), status
