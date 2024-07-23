module "cluster" {
  source = "./modules"

  instance_type = "c6in.metal" # t2.micro c6in.metal
  max_price     = 2.7765
  num_servers   = 2
  spot_instance = false
}
