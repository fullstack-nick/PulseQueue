output "instance_name" {
  value = google_compute_instance.pulsequeue.name
}

output "zone" {
  value = var.zone
}

output "public_ip" {
  value = google_compute_instance.pulsequeue.network_interface[0].access_config[0].nat_ip
}

output "api_url" {
  value = "http://${google_compute_instance.pulsequeue.network_interface[0].access_config[0].nat_ip}:8080"
}
