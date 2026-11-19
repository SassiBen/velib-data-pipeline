output "lake_bucket" {
  value = aws_s3_bucket.lake.bucket
}

output "msk_cluster_arn" {
  value = aws_msk_serverless_cluster.events.arn
}

output "spark_application_id" {
  value = aws_emrserverless_application.spark.id
}

output "spark_role_arn" {
  value = aws_iam_role.spark.arn
}
