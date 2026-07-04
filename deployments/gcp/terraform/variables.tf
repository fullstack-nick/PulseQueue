variable "project_id" {
  description = "Existing GCP project ID dedicated to PulseQueue."
  type        = string
}

variable "region" {
  description = "GCP region."
  type        = string
  default     = "us-central1"
}

variable "zone" {
  description = "GCP zone."
  type        = string
  default     = "us-central1-a"
}

variable "operator_cidr" {
  description = "Operator public IP CIDR allowed to reach SSH and API, for example 203.0.113.10/32."
  type        = string
}

variable "machine_type" {
  description = "Compute Engine VM type."
  type        = string
  default     = "e2-micro"
}

variable "boot_disk_size_gb" {
  description = "Boot disk size. Keep at 30 GB for GCP free-tier discipline."
  type        = number
  default     = 30
}
