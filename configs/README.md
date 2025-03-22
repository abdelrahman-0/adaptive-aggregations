A configuration file contains the ip addresses and ports of all participating workers nodes (plus an <i>optional</i> coordinator node if the adaptive implementation is being used).

Note that the order of the nodes in the <code>workers</code> array <b>MUST</b> match their <code>NODE_ID</code>'s, as in:
```json
{
  "workers" : [
    {
      "ip": "<ip of node 0>",
      "port": "..."
    },
    {
      "ip": "<ip of node 1>",
      "port": "..."
    }
  ]
}
```
