terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 4.66"
    }
  }
  required_version = ">= 1.2.0"
}

provider "aws" {
  region = "eu-central-1"
}
