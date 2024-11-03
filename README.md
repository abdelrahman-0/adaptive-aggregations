<p style="text-align:center">
<img src="logos/wip4.jpg" alt="grasshopper-db" width="12%"/>
</p>

install dependencies:
```shell
sudo apt-get -y update
sudo apt-get install cmake libgflags-dev liburing2 libjemalloc-dev

```

clone:
```shell
git clone https://github.com/abdelrahman-0/grasshopper-db.git
```

startup TODO:

```bash
cat /var/log/cloud-init-output.log
```

set `/proc/sys/net/core/rmem_max` to be large enough (e.g. 1<<28)
set `/proc/sys/net/core/wmem_max` to be large enough (e.g. 1<<28)\

echo 1 | sudo tee /proc/sys/kernel/sched_schedstats

TO DO:

- update datasketch dependency (local installation)
  - https://github.com/apache/datasketches-cpp/

- update option for compiling with:

`-stdlib=libc++` for forward-layout `std::tuple`s

`-stdlib=stdlibc++` for backward-layout `std::tuple`s
