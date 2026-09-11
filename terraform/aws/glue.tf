resource "aws_glue_job" "bronze_to_silver" {
  name        = "ecommerce-bronze-to-silver"
  description = "Processamento PySpark da camada Bronze para Silver do E-commerce Data Platform"
  role_arn    = "arn:aws:iam::585318732306:role/LabRole"

  glue_version      = "5.1"
  worker_type       = "G.1X"
  number_of_workers = 5
  timeout           = 60
  max_retries       = 0
  execution_class   = "STANDARD"

  default_arguments = {
    "--enable-continuous-cloudwatch-log" = "true"
    "--TempDir"                          = "s3://aws-glue-assets-585318732306-us-east-1/temporary/"
    "--conf"                             = "spark.eventLog.rolling.enabled=true --conf spark.sql.catalog.glue_catalog.glue.skip-name-validation=true"
    "--enable-glue-datacatalog"          = ""
    "--enable-metrics"                   = ""
    "--enable-job-insights"              = "true"
    "--enable-observability-metrics"     = "true"
    "--enable-spark-ui"                  = "true"
    "--job-bookmark-option"              = "job-bookmark-disable"
    "--job-language"                     = "python"
    "--spark-event-logs-path"            = "s3://aws-glue-assets-585318732306-us-east-1/sparkHistoryLogs/"
  }

  command {
    name            = "glueetl"
    script_location = "s3://aws-glue-assets-585318732306-us-east-1/scripts/ecommerce-bronze-to-silver.py"
    python_version  = "3"
  }

  execution_property {
    max_concurrent_runs = 1
  }
}

resource "aws_glue_job" "silver_to_gold" {
  name     = "ecommerce-silver-to-gold"
  role_arn = "arn:aws:iam::585318732306:role/LabRole"

  glue_version      = "5.1"
  worker_type       = "G.1X"
  number_of_workers = 5
  timeout           = 60
  max_retries       = 0
  execution_class   = "STANDARD"

  default_arguments = {
    "--TempDir"                          = "s3://aws-glue-assets-585318732306-us-east-1/temporary/"
    "--conf"                             = "spark.eventLog.rolling.enabled=true --conf spark.sql.catalog.glue_catalog.glue.skip-name-validation=true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-glue-datacatalog"          = ""
    "--enable-job-insights"              = "true"
    "--enable-metrics"                   = ""
    "--enable-observability-metrics"     = "true"
    "--enable-spark-ui"                  = "true"
    "--job-bookmark-option"              = "job-bookmark-disable"
    "--job-language"                     = "python"
    "--spark-event-logs-path"            = "s3://aws-glue-assets-585318732306-us-east-1/sparkHistoryLogs/"
  }

  command {
    name            = "glueetl"
    script_location = "s3://aws-glue-assets-585318732306-us-east-1/scripts/ecommerce-silver-to-gold.py"
    python_version  = "3"
  }

  execution_property {
    max_concurrent_runs = 1
  }
}


resource "aws_glue_job" "ml_preparation_v2" {
  name     = "ecommerce-ml-preparation-v2"
  role_arn = "arn:aws:iam::585318732306:role/LabRole"

  glue_version      = "5.1"
  worker_type       = "G.1X"
  number_of_workers = 10
  timeout           = 60
  max_retries       = 0
  execution_class   = "STANDARD"

  default_arguments = {
    "--TempDir"                          = "s3://aws-glue-assets-585318732306-us-east-1/temporary/"
    "--conf"                             = "spark.eventLog.rolling.enabled=true --conf spark.sql.catalog.glue_catalog.glue.skip-name-validation=true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-glue-datacatalog"          = ""
    "--enable-job-insights"              = "true"
    "--enable-metrics"                   = ""
    "--enable-observability-metrics"     = "true"
    "--enable-spark-ui"                  = "true"
    "--job-bookmark-option"              = "job-bookmark-disable"
    "--job-language"                     = "python"
    "--spark-event-logs-path"            = "s3://aws-glue-assets-585318732306-us-east-1/sparkHistoryLogs/"
  }

  command {
    name            = "glueetl"
    script_location = "s3://aws-glue-assets-585318732306-us-east-1/scripts/ecommerce-ml-preparation-v2.py"
    python_version  = "3"
  }

  execution_property {
    max_concurrent_runs = 1
  }
}


resource "aws_glue_job" "ml_training_v2" {
  name     = "ecommerce-ml-training-v2"
  role_arn = "arn:aws:iam::585318732306:role/LabRole"

  glue_version      = "5.1"
  worker_type       = "G.1X"
  number_of_workers = 10
  timeout           = 60
  max_retries       = 0
  execution_class   = "STANDARD"

  default_arguments = {
    "--TempDir"                          = "s3://aws-glue-assets-585318732306-us-east-1/temporary/"
    "--conf"                             = "spark.eventLog.rolling.enabled=true --conf spark.sql.catalog.glue_catalog.glue.skip-name-validation=true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-glue-datacatalog"          = ""
    "--enable-job-insights"              = "true"
    "--enable-metrics"                   = ""
    "--enable-observability-metrics"     = "true"
    "--enable-spark-ui"                  = "true"
    "--job-bookmark-option"              = "job-bookmark-disable"
    "--job-language"                     = "python"
    "--spark-event-logs-path"            = "s3://aws-glue-assets-585318732306-us-east-1/sparkHistoryLogs/"
  }

  command {
    name            = "glueetl"
    script_location = "s3://aws-glue-assets-585318732306-us-east-1/scripts/ecommerce-ml-training-v2.py"
    python_version  = "3"
  }

  execution_property {
    max_concurrent_runs = 1
  }
}

