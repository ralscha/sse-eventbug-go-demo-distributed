# sse-eventbug-go-demo-distributed

Two Go backends connected through Valkey pub/sub, matching the Java
`sse-eventbus-demo-distributed` application. The Vite client connects to both
nodes simultaneously.

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

After `task client-build`, either node also serves the production client; with
the default node ports, open `http://localhost:8080`.

Run `task test` for backend tests or `task build` to build both applications.

## License

MIT License. See [LICENSE](LICENSE) for details.
