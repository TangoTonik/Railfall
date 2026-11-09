"""Build a SparkSession that reads and writes the MinIO lake through S3A.

The configuration is a plain dict (`lake_conf`) so it can be unit-tested without a
JVM; `build_session` only applies it. Spark 4.0 ships Hadoop 3.4.1, so the
matching `hadoop-aws` artifact is pulled at start-up via `spark.jars.packages`
(it brings the AWS SDK v2 bundle with it). MinIO needs path-style access, and the
local stack speaks plain HTTP, so SSL is switched off only for `http://` endpoints.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from railfall.config import Settings

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

HADOOP_AWS_PACKAGE = "org.apache.hadoop:hadoop-aws:3.4.1"


def lake_uri(settings: Settings, key: str = "") -> str:
    """`s3a://` URI of a key in the lake bucket."""
    return f"s3a://{settings.lake_bucket}/{key.lstrip('/')}"


def lake_conf(settings: Settings) -> dict[str, str]:
    """Spark/Hadoop options for S3A access to the lake described by `settings`."""
    secure = settings.lake_endpoint_url.startswith("https://")
    return {
        "spark.jars.packages": HADOOP_AWS_PACKAGE,
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.endpoint": settings.lake_endpoint_url,
        "spark.hadoop.fs.s3a.access.key": settings.lake_access_key,
        "spark.hadoop.fs.s3a.secret.key": settings.lake_secret_key,
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.connection.ssl.enabled": str(secure).lower(),
        "spark.hadoop.fs.s3a.aws.credentials.provider": (
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
        ),
        # Overwrite only the partitions a run produces, never the whole table.
        "spark.sql.sources.partitionOverwriteMode": "dynamic",
        # Months and days are calendar values; keep them out of the JVM's zone.
        "spark.sql.session.timeZone": "UTC",
    }


def build_session(
    settings: Settings | None = None,
    *,
    app_name: str = "railfall",
    master: str = "local[*]",
    with_lake: bool = True,
) -> SparkSession:
    """Start (or reuse) a SparkSession; `with_lake=False` skips S3A for local-only work."""
    from pyspark.sql import SparkSession

    builder = SparkSession.builder.appName(app_name).master(master)
    conf = (
        lake_conf(settings or Settings.from_env())
        if with_lake
        else {
            "spark.sql.session.timeZone": "UTC",
            "spark.sql.sources.partitionOverwriteMode": "dynamic",
        }
    )
    for key, value in conf.items():
        builder = builder.config(key, value)
    session = builder.getOrCreate()
    session.sparkContext.setLogLevel("WARN")
    return session
