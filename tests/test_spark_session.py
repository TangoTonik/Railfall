"""Unit tests for the S3A configuration of railfall.spark.session (no JVM needed)."""

from __future__ import annotations

from railfall.config import Settings
from railfall.spark.session import HADOOP_AWS_PACKAGE, lake_conf, lake_uri


def test_lake_conf_points_s3a_at_minio_with_path_style() -> None:
    settings = Settings(lake_endpoint_url="http://minio:9000", lake_access_key="k")
    conf = lake_conf(settings)
    assert conf["spark.hadoop.fs.s3a.endpoint"] == "http://minio:9000"
    assert conf["spark.hadoop.fs.s3a.access.key"] == "k"
    assert conf["spark.hadoop.fs.s3a.path.style.access"] == "true"
    assert conf["spark.hadoop.fs.s3a.connection.ssl.enabled"] == "false"
    assert conf["spark.jars.packages"] == HADOOP_AWS_PACKAGE
    assert conf["spark.sql.sources.partitionOverwriteMode"] == "dynamic"


def test_ssl_follows_the_endpoint_scheme() -> None:
    conf = lake_conf(Settings(lake_endpoint_url="https://s3.example.org"))
    assert conf["spark.hadoop.fs.s3a.connection.ssl.enabled"] == "true"


def test_lake_uri_joins_bucket_and_key() -> None:
    settings = Settings(lake_bucket="lake")
    assert lake_uri(settings, "/silver/sncf/punctuality") == "s3a://lake/silver/sncf/punctuality"
    assert lake_uri(settings) == "s3a://lake/"


def test_secret_never_shows_in_settings_repr() -> None:
    assert "railfall-secret" not in repr(Settings())
