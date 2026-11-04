# Warehouse layout: one schema per stage of the ELT flow and one role per actor.
#
#   raw      <- written by the Spark/Python loader (railfall_loader)
#   staging  <- built by dbt from raw            (railfall_transformer)
#   marts    <- built by dbt, read by Metabase   (railfall_reader)
#
# Roles are LOGIN roles with no superuser/createdb/createrole rights.

locals {
  warehouse_schemas = ["raw", "staging", "marts"]

  warehouse_roles = {
    loader = {
      name     = "railfall_loader"
      password = var.warehouse_loader_password
    }
    transformer = {
      name     = "railfall_transformer"
      password = var.warehouse_transformer_password
    }
    reader = {
      name     = "railfall_reader"
      password = var.warehouse_reader_password
    }
  }

  # Which role may write (create tables in) which schema, and which may only read.
  schema_writers = {
    raw     = "loader"
    staging = "transformer"
    marts   = "transformer"
  }

  schema_readers = {
    raw     = ["transformer"]
    staging = []
    marts   = ["reader"]
  }

  read_grants = merge([
    for schema, readers in local.schema_readers : {
      for role in readers : "${schema}.${role}" => { schema = schema, role = role }
    }
  ]...)
}

resource "postgresql_role" "pipeline" {
  for_each = local.warehouse_roles

  name             = each.value.name
  login            = true
  password         = each.value.password
  superuser        = false
  create_database  = false
  create_role      = false
  connection_limit = 20
}

resource "postgresql_schema" "warehouse" {
  for_each = toset(local.warehouse_schemas)

  name     = each.key
  database = var.postgres_db
  # Writer owns its schema so dbt / the loader can create and replace tables
  # without grants on every new relation.
  owner = postgresql_role.pipeline[local.schema_writers[each.key]].name

  # Keep data on `terraform destroy` unless the schema is empty.
  drop_cascade = false
}

resource "postgresql_grant" "schema_usage" {
  for_each = local.read_grants

  database    = var.postgres_db
  role        = postgresql_role.pipeline[each.value.role].name
  schema      = postgresql_schema.warehouse[each.value.schema].name
  object_type = "schema"
  privileges  = ["USAGE"]
}

resource "postgresql_grant" "tables_select" {
  for_each = local.read_grants

  database    = var.postgres_db
  role        = postgresql_role.pipeline[each.value.role].name
  schema      = postgresql_schema.warehouse[each.value.schema].name
  object_type = "table"
  privileges  = ["SELECT"]
}

# Tables created later by the writer role are readable without re-applying.
resource "postgresql_default_privileges" "tables_select" {
  for_each = local.read_grants

  database    = var.postgres_db
  owner       = postgresql_role.pipeline[local.schema_writers[each.value.schema]].name
  role        = postgresql_role.pipeline[each.value.role].name
  schema      = postgresql_schema.warehouse[each.value.schema].name
  object_type = "table"
  privileges  = ["SELECT"]
}
