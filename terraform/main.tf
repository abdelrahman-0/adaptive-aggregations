module "cluster" {
  source = "./modules"

  instance_type = "c6in.16xlarge" # t2.micro c6in.8xlarge c6in.16xlarge c6gn.16xlarge
  max_price     = 2.9
  num_servers   = 2 # TODO ensure same AZ
  spot_instance = false
}
