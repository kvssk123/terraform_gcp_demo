variable "credentials" {
  description = "My credentials"
  default     = "/Users/saikumarkattera/Downloads/terrademo/keys/mycreds.json"
}

variable "project_name" {
  description = "Name of the project"
  default     = "My First Project"
}

variable "project_region" {
  description = "region of the project"
  default     = "us-centrall"
}

variable "location" {
  description = "Project location"
  default     = "US"
}

variable "bq_dataset_name" {
  description = "My BigQuery dataset name"
  default     = "cities_raw_data"
}

variable "composer_service_account" {
  description = "Service account for composer"
  default     = "terraform-temp@gleaming-modem-414522.iam.gserviceaccount.com"
}