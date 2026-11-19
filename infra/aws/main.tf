# Équivalent AWS de la pile locale. Non appliqué par défaut : la CI se contente
# de `terraform validate`. Correspondances :
#   Garage (S3)    -> S3
#   Kafka          -> Amazon MSK Serverless
#   Spark          -> EMR Serverless
#   métadonnées    -> Glue Data Catalog
terraform {
  required_version = ">= 1.6"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.70"
    }
  }
}

provider "aws" {
  region = var.region
  default_tags {
    tags = { project = var.project }
  }
}

resource "aws_s3_bucket" "lake" {
  bucket = "${var.project}-lake-${var.suffix}"
}

resource "aws_s3_bucket_public_access_block" "lake" {
  bucket                  = aws_s3_bucket.lake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_versioning" "lake" {
  bucket = aws_s3_bucket.lake.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "aws:kms"
    }
  }
}

# La couche bronze ne sert qu'aux recalculs : elle part en stockage froid.
resource "aws_s3_bucket_lifecycle_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id
  rule {
    id     = "bronze-to-glacier"
    status = "Enabled"
    filter {
      prefix = "bronze/"
    }
    transition {
      days          = 30
      storage_class = "GLACIER_IR"
    }
  }
}

resource "aws_msk_serverless_cluster" "events" {
  cluster_name = "${var.project}-events"
  vpc_config {
    subnet_ids         = var.subnet_ids
    security_group_ids = var.security_group_ids
  }
  client_authentication {
    sasl {
      iam {
        enabled = true
      }
    }
  }
}

resource "aws_glue_catalog_database" "lake" {
  name = replace("${var.project}_lake", "-", "_")
}

resource "aws_emrserverless_application" "spark" {
  name          = "${var.project}-spark"
  release_label = "emr-7.5.0"
  type          = "spark"
  maximum_capacity {
    cpu    = "8 vCPU"
    memory = "32 GB"
  }
  auto_stop_configuration {
    enabled              = true
    idle_timeout_minutes = 15
  }
}

data "aws_iam_policy_document" "emr_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["emr-serverless.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "spark" {
  name               = "${var.project}-spark"
  assume_role_policy = data.aws_iam_policy_document.emr_assume.json
}

data "aws_iam_policy_document" "spark" {
  statement {
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"]
    resources = [aws_s3_bucket.lake.arn, "${aws_s3_bucket.lake.arn}/*"]
  }
  statement {
    actions   = ["kafka-cluster:Connect", "kafka-cluster:DescribeTopic", "kafka-cluster:ReadData", "kafka-cluster:DescribeGroup", "kafka-cluster:AlterGroup"]
    resources = ["*"]
  }
  statement {
    actions   = ["glue:GetDatabase", "glue:GetTable", "glue:CreateTable", "glue:UpdateTable"]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "spark" {
  role   = aws_iam_role.spark.id
  policy = data.aws_iam_policy_document.spark.json
}
