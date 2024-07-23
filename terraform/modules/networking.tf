resource "aws_vpc" "main_vpc" {
  cidr_block = "10.0.0.0/16"
  tags = {
    Name = "VPC (Grasshopper DB)"
  }
}

resource "aws_subnet" "public_subnet" {
  vpc_id     = aws_vpc.main_vpc.id
  cidr_block = "10.0.0.0/24"
  tags = {
    Name = "Subnet (Grasshopper DB)"
  }
}

resource "aws_internet_gateway" "internet_gateway" {
  vpc_id = aws_vpc.main_vpc.id
  tags = {
    Name = "Internet Gateway (Grasshopper DB)"
  }
}

resource "aws_route_table" "route_table" {
  vpc_id = aws_vpc.main_vpc.id

  route {
    cidr_block = "10.0.0.0/16"
    gateway_id = "local"
  }

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.internet_gateway.id
  }

  tags = {
    Name = "Route table (Grasshopper DB)"
  }
}

resource "aws_route_table_association" "route_table_association" {
  subnet_id      = aws_subnet.public_subnet.id
  route_table_id = aws_route_table.route_table.id
}

resource "aws_security_group" "security_group" {
  description = "Allow all"
  vpc_id      = aws_vpc.main_vpc.id

  tags = {
    Name = "Security Group (Grasshopper DB)"
  }
}

resource "aws_vpc_security_group_ingress_rule" "allow_all_traffic_ipv4" {
  security_group_id = aws_security_group.security_group.id
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "-1" # semantically equivalent to all ports
}

resource "aws_vpc_security_group_egress_rule" "allow_all_traffic_ipv4" {
  security_group_id = aws_security_group.security_group.id
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "-1" # semantically equivalent to all ports
}
