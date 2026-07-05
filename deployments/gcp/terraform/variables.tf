variable "project_id" {
  description = "Existing GCP project ID dedicated to PulseQueue."
  type        = string
}

variable "region" {
  description = "GCP region."
  type        = string
  default     = "us-central1"

  validation {
    condition     = var.region == "us-central1"
    error_message = "PulseQueue's strict free-tier proof path is locked to us-central1."
  }
}

variable "zone" {
  description = "GCP zone."
  type        = string
  default     = "us-central1-a"

  validation {
    condition     = var.zone == "us-central1-a"
    error_message = "PulseQueue's strict free-tier proof path is locked to us-central1-a."
  }
}

variable "operator_cidr" {
  description = "Operator public IP CIDR allowed to reach SSH and API, for example 203.0.113.10/32."
  type        = string

  validation {
    condition     = can(cidrhost(var.operator_cidr, 0)) && endswith(var.operator_cidr, "/32")
    error_message = "operator_cidr must be a single-host /32 CIDR."
  }
}

variable "machine_type" {
  description = "Compute Engine VM type."
  type        = string
  default     = "e2-micro"

  validation {
    condition     = var.machine_type == "e2-micro"
    error_message = "PulseQueue's strict free-tier proof path requires e2-micro."
  }
}

variable "boot_disk_size_gb" {
  description = "Boot disk size. Keep at 30 GB for GCP free-tier discipline."
  type        = number
  default     = 30

  validation {
    condition     = var.boot_disk_size_gb > 0 && var.boot_disk_size_gb <= 30
    error_message = "boot_disk_size_gb must stay at or below 30 GB for free-tier discipline."
  }
}
