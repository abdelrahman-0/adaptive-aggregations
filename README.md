<p style="text-align:center">
<img src="logos/logo.png" alt="grasshopper-db" width="60%"/>
</p>

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