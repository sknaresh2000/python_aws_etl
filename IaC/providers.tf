terraform {
  required_version = "0.13.1"
  required_providers {
    aws = {
      version = "3.8.0"
      source  = "hashicorp/aws"
    }
  }
}

provider "aws" {
  assume_role {
    role_arn = var.workspace_iam_roles[terraform.workspace]
    }
  region  = var.region
}
