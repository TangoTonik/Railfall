# Metabase application database. Metabase keeps its own state (users, saved
# questions, dashboards) in Postgres instead of the default embedded H2 file, so
# the dashboards provisioned as code survive a container rebuild and the whole
# stack keeps a single database server. Like Airflow's, it is a separate
# database owned by a dedicated role: Metabase's migrations never touch the
# warehouse, and Metabase reads the marts only as `railfall_reader`.

resource "postgresql_role" "metabase" {
  name             = var.metabase_db_user
  login            = true
  password         = var.metabase_db_password
  superuser        = false
  create_database  = false
  create_role      = false
  connection_limit = 20
}

resource "postgresql_database" "metabase" {
  name              = var.metabase_db_name
  owner             = postgresql_role.metabase.name
  encoding          = "UTF8"
  template          = "template0"
  allow_connections = true
  connection_limit  = -1
}

resource "postgresql_grant" "metabase_revoke_public" {
  database    = postgresql_database.metabase.name
  role        = "public"
  object_type = "database"
  privileges  = []
}
