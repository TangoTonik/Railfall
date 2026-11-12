# Airflow metadata database. It lives on the same Postgres server as the
# warehouse but in its own database, owned by its own role: Airflow's migrations
# create ~50 tables, and none of them belong next to raw/staging/marts. The
# airflow role cannot see the warehouse database's schemas, and the pipeline
# roles cannot connect to this one.

resource "postgresql_role" "airflow" {
  name             = var.airflow_db_user
  login            = true
  password         = var.airflow_db_password
  superuser        = false
  create_database  = false
  create_role      = false
  connection_limit = 50
}

resource "postgresql_database" "airflow" {
  name              = var.airflow_db_name
  owner             = postgresql_role.airflow.name
  encoding          = "UTF8"
  template          = "template0"
  allow_connections = true
  connection_limit  = -1
}

# New databases grant CONNECT to PUBLIC; take it back so only the owner gets in.
resource "postgresql_grant" "airflow_revoke_public" {
  database    = postgresql_database.airflow.name
  role        = "public"
  object_type = "database"
  privileges  = []
}
