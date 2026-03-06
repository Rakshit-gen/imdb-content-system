def test_health_check(client):
    resp = client.get("/health")
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["status"] == "ok"


def test_not_found(client):
    resp = client.get("/nonexistent")
    assert resp.status_code == 404
    body = resp.get_json()
    assert "error" in body
