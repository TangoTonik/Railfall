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
