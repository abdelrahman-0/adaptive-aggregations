<h1><code><b>Grasshopper DB</b></code></h1>

<p align="center">
<img src="logos/wip4.jpg" alt="grasshopper-db" width="20%"/>


<h3><code>Current Scope: Adaptive Aggregation</code></h3>
<h6>see micro-benchmarks/aggregation folder</h6>
<ol>
<li>local aggregation (done)</li>
<li>homogeneous aggregation (WIP)</li>
<li>heterogeneous aggregation (WIP)</li>
<li>adaptive (not started)</li>
</ol>
<h4>Future TODOs (out of scope for now):</h4>
<ol>
<li>SSD Spilling</li>
<li>Scanning from S3</li>
<li>Adaptive Joins</li>
<li>Distributed Query Optimization</li>
</ol>
</p>
<details open>
<summary><code><b>Getting Started</b></code></summary>
<ul>
<li>
clone repository:

```shell
git clone https://github.com/abdelrahman-0/grasshopper-db.git
```
</li>

<li>
install dependencies (note some dependencies are installed from source in your <code>home</code> dir):

```shell
cd grasshopper-db
make install
```
</li>
</ul>
</details>

<details>
<summary><code><b>Additional Remarks</b></code></summary>
startup TODO:

Check if deps were installed correctly on your EC2 instance
```bash
cat /var/log/cloud-init-output.log
```

set `/proc/sys/net/core/rmem_max` to be large enough (e.g. 1<<28)
set `/proc/sys/net/core/wmem_max` to be large enough (e.g. 1<<28)\
echo 1 | sudo tee /proc/sys/kernel/sched_schedstats

- update option for compiling with:

`-stdlib=libc++` for forward-layout `std::tuple`s

`-stdlib=stdlibc++` for backward-layout `std::tuple`s

use `--no-pin` with `LIKWID` target

</details>
