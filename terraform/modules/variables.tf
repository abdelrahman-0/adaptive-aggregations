variable "max_price" {
  description = "Maximum price for instance ($)"
  type        = number
  default     = 0.8
}

variable "spot_instance" {
  description = "Whether to use a spot instance or not"
  type        = bool
  default     = "true"
}

variable "instance_type" {
  description = "Type of EC2 instance"
  type        = string
  default     = "i3en.6xlarge"
}

variable "num_servers" {
  description = "Number of instances"
  type        = number
  default     = 1
}

variable "ssh_key" {
  description = "Name of ssh key pair used to connect to the EC2 instance(s)"
  type        = string
  default     = "ssh key"
}