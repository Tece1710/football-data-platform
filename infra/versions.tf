terraform {
  required_version = ">= 1.6"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }
}

provider "aws" {
  region = var.aws_region

  # Tags applied automatically to every resource this provider creates
  default_tags {
    tags = {
      Project   = "football-data-platform"
      ManagedBy = "terraform"
    }
  }
}
