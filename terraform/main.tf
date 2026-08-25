terraform {
  required_providers {
    docker = {
      source  = "kreuzwerker/docker"
      version = "~> 3.8"
    }
  }
}

provider "docker" {
  host = "unix:///var/run/docker.sock"
}

resource "docker_network" "stock_network" {
  name = "stock_network_tf"
}

resource "docker_volume" "postgres_data" {
  name = "postgres_data_tf"
}

resource "docker_volume" "minio_data" {
  name = "minio_data_tf"
}

resource "docker_image" "postgres" {
  name         = "postgres:15"
  keep_locally = true
}

resource "docker_image" "minio" {
  name         = "minio/minio:latest"
  keep_locally = true
}

resource "docker_container" "postgres" {
  name  = "postgres_tf"
  image = docker_image.postgres.image_id

  env = [
    "POSTGRES_DB=marketdb",
    "POSTGRES_USER=marketuser",
    "POSTGRES_PASSWORD=marketpass"
  ]

  ports {
    internal = 5432
    external = 5433
  }

  volumes {
    volume_name    = docker_volume.postgres_data.name
    container_path = "/var/lib/postgresql/data"
  }

  networks_advanced {
    name = docker_network.stock_network.name
  }
}

resource "docker_container" "minio" {
  name  = "minio_tf"
  image = docker_image.minio.image_id

  env = [
    "MINIO_ROOT_USER=minioadmin",
    "MINIO_ROOT_PASSWORD=minioadmin"
  ]

  command = ["server", "/data", "--console-address", ":9001"]

  ports {
    internal = 9000
    external = 9002
  }

  ports {
    internal = 9001
    external = 9003
  }

  volumes {
    volume_name    = docker_volume.minio_data.name
    container_path = "/data"
  }

  networks_advanced {
    name = docker_network.stock_network.name
  }
}
