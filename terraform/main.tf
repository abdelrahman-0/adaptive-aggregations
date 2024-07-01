module "cluster" {
  source = "./modules"

  instance_type = "t2.micro"
  max_price     = 0.0047
  num_servers   = 2
}
