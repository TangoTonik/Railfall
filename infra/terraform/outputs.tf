output "lake_bucket" {
  description = "Name of the data lake bucket."
  value       = minio_s3_bucket.lake.bucket
}

output "lake_layers" {
  description = "Medallion layer prefixes created in the lake bucket."
  value       = [for layer in local.lake_layers : "${layer}/"]
}

output "lake_pipeline_user" {
  description = "Access key the pipelines use for the lake."
  value       = minio_iam_user.pipeline.name
}
