terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "5.24.0"
    }
  }
}

provider "google" {
  credentials = var.credentials
  project     = var.project_name
  region      = var.project_region
}

resource "google_storage_bucket" "demo-bucket" {
  name          = var.project_name
  location      = var.location
  force_destroy = true

  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}

resource "google_bigquery_dataset" "dataset_1" {
  dataset_id = "flights_raw_data"
  location   = var.location
}

resource "google_bigquery_dataset" "dataset_2" {
  dataset_id = "flights_datamart"
  location   = var.location
}

resource "google_composer_environment" "test" {
  name   = var.project_name
  region = "us-central1"
  config {

    software_config {
      image_version = "composer-2.6.4-airflow-2.6.3"
      pypi_packages = {
        dlt = "==0.4.7" # Specify version as required
        airflow-dbt = "==0.4.0"
        dbt-bigquery = "==0.21.0"
      }
      env_variables = {
        API_IATACODE_ACCESS_KEY = "your_access_key_here" # Replace with your IATA code and corresponding access key
      }
    }

    workloads_config {
      scheduler {
        cpu        = 0.5
        memory_gb  = 2
        storage_gb = 1
        count      = 1
      }
      triggerer {
        cpu       = 0.5
        memory_gb = 0.5
        count     = 1
      }
      web_server {
        cpu        = 0.5
        memory_gb  = 2
        storage_gb = 1
      }
      worker {
        cpu        = 0.5
        memory_gb  = 2
        storage_gb = 1
        min_count  = 1
        max_count  = 3
      }
    }

    environment_size = "ENVIRONMENT_SIZE_SMALL"

    node_config {
      service_account = var.composer_service_account
    }
  }
}