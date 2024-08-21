locals {
  instance_user_data = file("${path.module}/../../install_dependencies.sh")
}