provider "aws" {
  region = var.aws_region
}

resource "aws_glue_job" "delta_lakehouse_job" {
  name     = var.job_name
  role_arn = var.glue_role_arn

  command {
    name            = "glueetl"
    script_location = var.script_location
    python_version  = "3"
  }

  max_retries     = 1
  glue_version    = var.glue_version
  number_of_workers = var.number_of_workers
  worker_type     = var.worker_type

  default_arguments = {
    "--job-language"                       = "python"
    "--datalake-formats"                   = "delta"
    "--enable-glue-datacatalog"            = "true"
    "--enable-job-insights"                = "true"
    "--enable-observability-metrics"       = "true"
    "--enable-metrics"                     = "true"
    "--enable-spark-ui"                    = "true"
    "--enable-continuous-cloudwatch-log"   = "true"
    "--job-bookmark-option"                = "job-bookmark-disable"
    "--PRODUCTS_OUTPUT_PATH"               = var.products_output_path
    "--ORDER_ITEMS_OUTPUT_PATH"            = var.order_items_output_path
    "--ORDERS_OUTPUT_PATH"                 = var.orders_output_path
    "--REJECTED_PATH"                      = var.rejected_path
    "--S3_BUCKET_PATH"                     = var.s3_bucket_path
    "--LOG_LEVEL"                          = var.log_level
    "--TempDir"                             = var.temp_dir
    "--spark-event-logs-path"              = var.spark_event_logs_path
    "--conf"                                = "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog --conf spark.delta.logStore.class=org.apache.spark.sql.delta.storage.S3SingleDriverLogStore"
  }
}

variable "aws_region" {
  default = "eu-west-1"
}

variable "job_name" {}
variable "glue_role_arn" {}
variable "script_location" {}
variable "glue_version" {
  default = "5.0"
}
variable "number_of_workers" {
  default = 2
}
variable "worker_type" {
  default = "G.1X"
}

variable "products_output_path" {}
variable "order_items_output_path" {}
variable "orders_output_path" {}
variable "rejected_path" {}
variable "s3_bucket_path" {}
variable "log_level" {
  default = "INFO,ERROR,DEBUG"
}
variable "temp_dir" {}
variable "spark_event_logs_path" {}