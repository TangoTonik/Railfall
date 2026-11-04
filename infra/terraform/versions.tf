# Providers for the local platform. Versions are pinned so `init` is reproducible;
# the lock file (.terraform.lock.hcl) is committed once generated in CI.
terraform {
  required_version = ">= 1.6.0"

  required_providers {
    minio = {
      source  = "aminueza/minio"
      version = "~> 2.5.0"
    }
    postgresql = {
      source  = "cyrilgdn/postgresql"
      version = "~> 1.23.0"
    }
  }
}

provider "minio" {
  minio_server   = var.minio_server
  minio_user     = var.minio_root_user
  minio_password = var.minio_root_password
  minio_ssl      = false
}

# Terraform talks to the warehouse as the bootstrap superuser created by the
# Postgres image; everything it creates is owned by that user, and the pipeline
# roles below only get the grants they need.
provider "postgresql" {
  host            = var.postgres_host
  port            = var.postgres_port
  database        = var.postgres_db
  username        = var.postgres_admin_user
  password        = var.postgres_admin_password
  sslmode         = "disable"
  connect_timeout = 15
  # The image's POSTGRES_USER is a superuser, so ownership changes need no
  # temporary role grants.
  superuser = true
}
