resource "aws_s3_bucket" "data_bucket" {
  bucket = var.data_bucket_name

  tags = {
    Name        = "SupplyChain360 Data Lake"
    Environment = "dev"
  }
}

resource "aws_s3_object" "raw_folder" {
  bucket = aws_s3_bucket.data_bucket.id
  key    = "raw/"
}

resource "aws_s3_bucket_versioning" "data_versioning" {
  bucket = aws_s3_bucket.data_bucket.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_public_access_block" "data_block" {
  bucket = aws_s3_bucket.data_bucket.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}