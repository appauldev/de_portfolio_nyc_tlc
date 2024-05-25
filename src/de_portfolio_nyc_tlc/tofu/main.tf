terraform {
  required_providers {
    google = {
      source  = "opentofu/google"
      version = "5.30.0"
    }
  }
}

provider "google" {
  # Configuration options
  credentials = file(var.gcp_key)
  project     = var.project_id
}


resource "google_storage_bucket" "nyc_tlc" {
  name          = var.bucket_name
  location      = var.region
  storage_class = "STANDARD"
  force_destroy = true

  public_access_prevention    = "enforced"
  uniform_bucket_level_access = "true"

  lifecycle_rule {
    condition {
      age = 1
    }

    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }

  lifecycle_rule {
    condition {
      age = 15
    }

    action {
      type          = "SetStorageClass"
      storage_class = "COLDLINE"
    }
  }

}
