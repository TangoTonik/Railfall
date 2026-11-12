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


def test_lake_credentials_fall_back_to_minio_root():
    s = Settings.from_env({"MINIO_ROOT_USER": "admin", "MINIO_ROOT_PASSWORD": "pw"})
    assert (s.lake_access_key, s.lake_secret_key) == ("admin", "pw")


def test_explicit_lake_credentials_win():
    s = Settings.from_env(
        {
            "MINIO_ROOT_USER": "admin",
            "MINIO_ROOT_PASSWORD": "pw",
            "LAKE_ACCESS_KEY": "app",
            "LAKE_SECRET_KEY": "app-pw",
        }
    )
    assert (s.lake_access_key, s.lake_secret_key) == ("app", "app-pw")


def test_secrets_hidden_from_repr():
    text = repr(
        Settings.from_env({"LAKE_SECRET_KEY": "s3cr3t", "WAREHOUSE_DSN": "postgresql://u:pw@h/db"})
    )
    assert "s3cr3t" not in text
    assert "pw@h" not in text


def test_meteo_departments_default_to_rail_hubs():
    assert Settings.from_env({}).meteo_departments == ("75", "69", "13")


def test_meteo_departments_parsed_and_deduplicated():
    s = Settings.from_env({"METEO_DEPARTMENTS": " 33, 2A,33 ,,6"})
    assert s.meteo_departments == ("33", "2A", "6")


def test_empty_meteo_departments_rejected():
    with pytest.raises(ValueError, match="METEO_DEPARTMENTS"):
        Settings.from_env({"METEO_DEPARTMENTS": " , "})


def test_airflow_url_follows_the_published_web_port():
    assert Settings.from_env({}).airflow_url == "http://localhost:8080"
    assert Settings.from_env({"AIRFLOW_WEB_PORT": "8081"}).airflow_url == "http://localhost:8081"
    explicit = Settings.from_env({"AIRFLOW_URL": "http://airflow:8080/", "AIRFLOW_WEB_PORT": "1"})
    assert explicit.airflow_url == "http://airflow:8080"


def test_airflow_password_hidden_from_repr():
    s = Settings.from_env({"AIRFLOW_ADMIN_USER": "ops", "AIRFLOW_ADMIN_PASSWORD": "hunter2"})
    assert s.airflow_user == "ops"
    assert "hunter2" not in repr(s)
