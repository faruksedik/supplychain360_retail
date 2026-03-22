terraform {
  backend "s3" {
    bucket  = "faruk-supplychain360-tf-state-bucket"
    key     = "infra/terraform.tfstate"
    region  = "us-east-1"
    profile = "destination-account"
    encrypt = true
  }
}