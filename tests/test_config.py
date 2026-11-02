import pytest

from railfall.config import Settings


def test_defaults_when_env_empty():
    s = Settings.from_env({})
    assert s == Settings()
    assert s.lake_bucket == "railfall-lake"


def test_env_overrides():
    s = Settings.from_env(
        {
            "LAKE_ENDPOINT_URL": "https://s3.eu-west-3.amazonaws.com",
            "LAKE_BUCKET": "prod-lake",
            "HTTP_TIMEOUT_S": "5.5",
            "HTTP_MAX_RETRIES": "0",
        }
    )
    assert s.lake_endpoint_url == "https://s3.eu-west-3.amazonaws.com"
    assert s.lake_bucket == "prod-lake"
    assert s.http_timeout_s == 5.5
    assert s.http_max_retries == 0


def test_negative_retries_rejected():
    with pytest.raises(ValueError):
        Settings.from_env({"HTTP_MAX_RETRIES": "-1"})
