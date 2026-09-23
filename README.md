# protocol-proxy-nats

NATS plugin for [protocol-proxy](https://github.com/eclipse-volttron/lib-protocol-proxy). A
`ProtocolProxyManager` launches one `NATSProxy` subprocess per NATS connection:

```python
from protocol_proxy.manager.gevent import GeventProtocolProxyManager
manager = GeventProtocolProxyManager.get_manager('nats')   # resolves protocol_proxy.protocol.nats.PROXY_CLASS
peer = manager.get_proxy(('nats', 'nats://localhost:4222'), servers='nats://localhost:4222')
manager.wait_peer_registered(peer, timeout=30)
```

Keyword arguments to `get_proxy` become command line options of the proxy process:
`servers` (comma-separated URLs), `name`, `user`, `password`, `nats_token`, `connect_timeout`,
`max_reconnect_attempts`, `reconnect_time_wait`, `tls`.

The proxy connects to NATS before registering with the manager. If no server is reachable the
process exits with a non-zero code and the manager logs the failure.

## Messages

| Direction | Method | Payload (JSON) |
| --- | --- | --- |
| server → manager | `PUBLISH_LOCAL` | `{"topic": subject, "payload": <hex bytes>, "headers", "reply"}` |
| manager → server | `PUBLISH_REMOTE` | `{"topic", "payload", "headers"?}` |
| manager → server | `SUBSCRIBE_REMOTE` | `{"topics": [subject, ...]}` |
| manager → server | `UNSUBSCRIBE_REMOTE` | `{"topics": [subject, ...]}` |

String payloads are sent as UTF-8; other JSON values are serialized with `json.dumps`. NATS has
no QoS, so `[subject, qos]` entries are accepted and the QoS ignored.

## Development

```bash
pip install -e .
pytest
```
