install dependencies:
```shell
sudo apt-get install libgflags-dev liburing2
```

clone:
```shell
git clone repo
```

startup TODO:

set `/proc/sys/net/core/rmem_max` to be large enough (e.g. 1<<28)