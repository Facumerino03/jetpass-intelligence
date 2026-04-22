from fastapi.testclient import TestClient


def test_healthcheck(client: TestClient) -> None:
    response = client.get("/api/v1/health/")

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "ok"
    assert "service" in body


def test_readiness_check(client: TestClient) -> None:
    response = client.get("/api/v1/health/ready")

    assert response.status_code == 200
    body = response.json()
    assert body["status"] in {"ok", "degraded"}
    assert body["database"] in {"ok", "not_configured", "error"}
    assert body["redis"] in {"ok", "not_configured", "error"}
