variable "aws_region" {
  description = "AWS region where resources will be created"
  type        = string
  default     = "us-east-1"
}

variable "data_bucket_name" {
  description = "Name of the S3 bucket used to store parquet files for the SupplyChain360 data lake"
  type        = string
  default     = "faruk-supplychain360-bucket"
}

variable "snowflake_account" {
  description = "Snowflake account identifier (e.g., xy12345.eu-west-1)"
  type        = string
}

variable "snowflake_organization_name" {
  description = "Snowflake organization name"
  type        = string
}

variable "snowflake_user" {
  description = "Snowflake username used for authentication"
  type        = string
}

variable "snowflake_password" {
  description = "Password for the Snowflake user"
  type        = string
  sensitive   = true
}

variable "snowflake_role" {
  description = "Snowflake role with permissions to create resources"
  type        = string
  default     = "ACCOUNTADMIN"
}