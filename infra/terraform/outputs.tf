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

output "warehouse_schemas" {
  description = "Schemas created in the warehouse database."
  value       = sort([for s in postgresql_schema.warehouse : s.name])
}

output "warehouse_roles" {
  description = "Pipeline role names keyed by actor (loader, transformer, reader)."
  value       = { for k, r in postgresql_role.pipeline : k => r.name }
}

output "airflow_database" {
  description = "Airflow metadata database and the role that owns it."
  value = {
    name  = postgresql_database.airflow.name
    owner = postgresql_database.airflow.owner
  }
}
