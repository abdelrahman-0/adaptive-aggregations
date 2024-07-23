module "cluster" {
  source = "./modules"

  instance_type = "c6in.8xlarge" # t2.micro c6in.8xlarge c6in.metal
  max_price     = 0.9
  num_servers   = 2 # TODO ensure same AZ
  spot_instance = false
}
