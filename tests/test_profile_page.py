from fastapi.testclient import TestClient

from web.app import app


def test_profile_page_route_exists():
    client = TestClient(app)
    resp = client.get('/profile')
    assert resp.status_code == 200
    assert 'Career Profile' in resp.text
