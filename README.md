<div align="center">
<h1> ⚡ Adaptive Aggregations </h1>

<img src="dpe.png" alt="" width="250"/>

<h6>
This repo implements an aggregation operator capable of scaling out <i>on-the-fly</i>.
That is, as the query executes, the engine might scale out <i>mid-execution</i>.
</h6>
</div>

---

### 📁 Structure

```
📂
├── adaptive-aggregation/
├── analysis/
├── configs/
...
├── micro-benchmarks/
│   ├── aggregation/
│   └── shuffle/
└── Makefile
```

---

### 🔧 Installation & Setup

The repo's dependencies can be found in <code>install_deps.sh</code> and can be installed as follows: 
```bash
make install
```

⚠️ Note that some libraries are installed from source and might overwrite local installations (e.g. <code>liburing</code>)

After installing the dependencies, you build all 3 targets:
using:
```bash
make all
```
or each target individually:
```bash
make shuffle
make aggregation
make adaptive
```

---

### 🛠️ Usage
HT + EC2 cluster/local + Entrypoint
```
```

