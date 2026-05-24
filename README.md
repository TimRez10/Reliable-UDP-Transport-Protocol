# Reliable UDP Protocol Project

A simple reliable data delivery protocol built on top of UDP. It uses sequence numbers, acknowledgments, retransmissions, and a proxy that can inject loss and delay to test recovery behavior.

## Scripts

### `client.py`
Interactive UDP client that reads messages from standard input and sends them reliably to the server. It waits for acknowledgments, retries messages when needed, and logs client activity locally and to the remote log collector.

### `server.py`
UDP server that receives client messages, prints them, and sends acknowledgments back. It also records packet activity so the protocol flow can be traced during testing.

### `proxy.py`
Network proxy between the client and server and forwards traffic in both directions. It can randomly drop or delay packets to simulate an unreliable network and exercise the retry logic.

### `reliable_udp.py`
Helper module that defines the packet format used by the protocol. It converts messages and sequence or acknowledgment numbers to and from bytes for transmission over UDP.

### `logger.py`
Shared logging setup used by the client, server, and proxy. It configures console logging and optional remote logging to the collector.

### `log_collector.py`
TCP log collector that receives JSON log entries from the running scripts and appends them to `proxy_logs.jsonl`. It provides a central place to capture protocol events for analysis.

### `visualizer.py`
Live dashboard that reads `proxy_logs.jsonl` and visualizes packet events over time. It helps show drops, retransmissions, acknowledgments, and other network behavior during a run.
