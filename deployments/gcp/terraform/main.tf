resource "google_service_account" "pulsequeue" {
  account_id   = "pulsequeue-vm"
  display_name = "PulseQueue VM service account"
}

resource "google_compute_network" "pulsequeue" {
  name                    = "pulsequeue-network"
  auto_create_subnetworks = true
}

resource "google_compute_firewall" "allow_operator_ssh" {
  name    = "pulsequeue-allow-operator-ssh"
  network = google_compute_network.pulsequeue.name

  allow {
    protocol = "tcp"
    ports    = ["22"]
  }

  source_ranges = [var.operator_cidr]
  target_tags   = ["pulsequeue"]
}

resource "google_compute_firewall" "allow_operator_api" {
  name    = "pulsequeue-allow-operator-api"
  network = google_compute_network.pulsequeue.name

  allow {
    protocol = "tcp"
    ports    = ["8080", "9090"]
  }

  source_ranges = [var.operator_cidr]
  target_tags   = ["pulsequeue"]
}

resource "google_compute_instance" "pulsequeue" {
  name         = "pulsequeue-phase1"
  machine_type = var.machine_type
  zone         = var.zone
  tags         = ["pulsequeue"]

  boot_disk {
    initialize_params {
      image = "debian-cloud/debian-12"
      size  = var.boot_disk_size_gb
      type  = "pd-standard"
    }
  }

  network_interface {
    network = google_compute_network.pulsequeue.name

    access_config {
    }
  }

  service_account {
    email  = google_service_account.pulsequeue.email
    scopes = ["https://www.googleapis.com/auth/cloud-platform"]
  }

  metadata = {
    enable-oslogin = "TRUE"
  }
}
