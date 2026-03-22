

# Create database
resource "snowflake_database" "db" {
  name = "SUPPLYCHAIN360_DB"

  comment = "Main supply chain database"
}

# Create RAW schema
resource "snowflake_schema" "raw" {
  database = snowflake_database.db.name
  name     = "RAW"

  comment = "Raw ingestion layer"
}