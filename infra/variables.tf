variable "aws_region" {
  description = "AWS region for all resources"
  type        = string
  default     = "eu-north-1" # Stockholm to match the region of my project
}

variable "raw_bucket_name" {
  description = "Name of the S3 bucket for raw API files. Must be globally unique."
  type        = string
}
