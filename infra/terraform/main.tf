terraform {
  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.50"
    }
  }
}

provider "databricks" {}

# --- Unity Catalog: catalog + three schemas ---------------------------------

resource "databricks_catalog" "main" {
  name         = "main"
  metastore_id = var.metastore_id
  comment      = "Root catalog for lakehouse-pattern"
}

resource "databricks_schema" "bronze" {
  catalog_name = databricks_catalog.main.name
  name         = "lakehouse_pattern_bronze"
  properties = { layer = "bronze", owner = "data-platform" }
}

resource "databricks_schema" "silver" {
  catalog_name = databricks_catalog.main.name
  name         = "lakehouse_pattern_silver"
  properties = { layer = "silver", owner = "data-platform", pii = "true" }
}

resource "databricks_schema" "gold" {
  catalog_name = databricks_catalog.main.name
  name         = "lakehouse_pattern_gold"
  properties = { layer = "gold", owner = "analytics" }
}

# --- Grants -----------------------------------------------------------------

resource "databricks_grants" "analysts_gold" {
  schema = databricks_schema.gold.id
  grant {
    principal  = "analysts"
    privileges = ["USE_SCHEMA", "SELECT"]
  }
}

resource "databricks_grants" "engineers_bronze" {
  schema = databricks_schema.bronze.id
  grant {
    principal  = "data-engineers"
    privileges = ["ALL_PRIVILEGES"]
  }
}

# --- Jobs cluster + SQL warehouse ------------------------------------------

resource "databricks_cluster" "jobs" {
  cluster_name  = "lakehouse-pattern-jobs"
  spark_version = "15.4.x-scala2.12"
  node_type_id  = "Standard_D4s_v5"
  autoscale { min_workers = 2, max_workers = 8 }
  runtime_engine = "PHOTON"
}

resource "databricks_sql_endpoint" "wh" {
  name                = "lakehouse-pattern-sqlwh"
  cluster_size        = "Small"
  auto_stop_mins      = 10
  warehouse_type      = "PRO"
  enable_photon       = true
  enable_serverless_compute = true
}
