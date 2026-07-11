# sse-eventbug-go-demo-distributed

Two Go backends connected through Valkey pub/sub, matching the Java
`sse-eventbus-demo-distributed` application. The Vite client is unchanged and
connects to both nodes simultaneously.

Run these tasks in separate terminals:

```text
task valkey
task node-a
task node-b
task client
```

Open `http://localhost:5173`. Messages sent through either node are delivered
to clients connected to both nodes. The Valkey transport is implemented with
the Go standard library and suppresses messages originating from the local
node.

## License

MIT License. See [LICENSE](LICENSE) for details.
