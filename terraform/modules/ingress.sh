#!/bin/bash

apt-get -y update

apt-get install git

git clone https://github.com/abdelrahman-0/grasshopper-db.git
cd grasshopper-db
./install_dependencies.sh
