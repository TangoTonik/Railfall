# Inputs come from TF_VAR_* environment variables (see .env.example), never from
# a committed tfvars file: the only defaults here are non-secret local values.

variable "minio_server" {
  description = "MinIO API endpoint as host:port, without scheme."
  type        = string
  default     = "localhost:9000"
}

variable "minio_root_user" {
  description = "MinIO admin user, used only by Terraform."
  type        = string
}

variable "minio_root_password" {
  description = "MinIO admin password, used only by Terraform."
  type        = string
  sensitive   = true
}

variable "lake_bucket" {
  description = "Data lake bucket holding the bronze and silver layers."
  type        = string
  default     = "railfall-lake"

  validation {
    condition     = can(regex("^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$", var.lake_bucket))
    error_message = "lake_bucket must be a valid S3 bucket name (3-63 chars, lowercase)."
  }
}

variable "lake_pipeline_user" {
  description = "Access key of the least-privilege user the pipelines run as."
  type        = string
  default     = "railfall-pipeline"
}

variable "lake_pipeline_secret" {
  description = "Secret key of the pipeline user (min. 8 chars, MinIO requirement)."
  type        = string
  sensitive   = true

  validation {
    condition     = length(var.lake_pipeline_secret) >= 8
    error_message = "lake_pipeline_secret must be at least 8 characters."
  }
}
