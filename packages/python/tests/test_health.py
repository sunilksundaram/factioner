from factioner import __version__, health


def test_health():
    payload = health()
    assert payload["status"] == "ok"
    assert payload["package"] == "factioner"
    assert payload["version"] == __version__
