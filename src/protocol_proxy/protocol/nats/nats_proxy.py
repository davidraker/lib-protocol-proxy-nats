"""NATS protocol proxy.

Runs as a subprocess launched by a ProtocolProxyManager and bridges a NATS server to the
manager's IPC protocol:

  server -> manager:  every received NATS message is forwarded as ``PUBLISH_LOCAL`` with
                      ``{'topic': subject, 'payload' (hex), 'headers', 'reply'}``.
  manager -> server:  ``PUBLISH_REMOTE`` ``{'topic', 'payload', 'headers'?}``,
                      ``SUBSCRIBE_REMOTE`` / ``UNSUBSCRIBE_REMOTE`` ``{'topics': [subject, ...]}``.
"""
import json
import logging
import ssl
import sys

from argparse import ArgumentParser
from typing import Any, Callable

import nats

from nats.aio.client import Client
from nats.aio.msg import Msg
from nats.aio.subscription import Subscription
from nats.errors import Error as NATSError

from protocol_proxy.ipc import async_callback, ProtocolHeaders, ProtocolProxyMessage
from protocol_proxy.proxy.asyncio import AsyncioProtocolProxy
from protocol_proxy.proxy.launch import launch, redact, str2bool

_log = logging.getLogger(__name__)

DEFAULT_SERVER = 'nats://localhost:4222'


class NATSProxy(AsyncioProtocolProxy):
    def __init__(self, *, servers: str | list[str] = DEFAULT_SERVER, name: str | None = None,
                 user: str | None = None, password: str | None = None, nats_token: str | None = None,
                 connect_timeout: float = 2.0, max_reconnect_attempts: int = -1, reconnect_time_wait: float = 2.0,
                 tls: bool = False, **kwargs):
        super().__init__(**kwargs)
        self.servers = self.parse_servers(servers)
        self.name = name or self.proxy_name
        self.user, self.password, self.nats_token = user, password, nats_token
        self.connect_timeout = connect_timeout
        self.max_reconnect_attempts = max_reconnect_attempts
        self.reconnect_time_wait = reconnect_time_wait
        self.tls = tls
        self.nc: Client | None = None
        self.subscriptions: dict[str, Subscription] = {}

        self.register_callback(self.handle_publish_remote, 'PUBLISH_REMOTE')
        self.register_callback(self.handle_subscribe_remote, 'SUBSCRIBE_REMOTE')
        self.register_callback(self.handle_unsubscribe_remote, 'UNSUBSCRIBE_REMOTE')

    @staticmethod
    def parse_servers(servers: str | list[str]) -> list[str]:
        """Accept a list or a comma-separated string (the form that survives the command line)."""
        if isinstance(servers, str):
            servers = servers.split(',')
        parsed = [s.strip() for s in servers if s and s.strip()]
        if not parsed:
            raise ValueError('At least one NATS server URL is required.')
        return parsed

    @staticmethod
    def topic_delimiter() -> str:
        return '.'

    @classmethod
    def get_unique_remote_id(cls, unique_remote_id: tuple) -> tuple:
        return unique_remote_id

    ##################
    # Server connection
    ##################

    def _connect_options(self) -> dict[str, Any]:
        options: dict[str, Any] = dict(servers=self.servers, name=self.name, connect_timeout=self.connect_timeout,
                                       max_reconnect_attempts=self.max_reconnect_attempts,
                                       reconnect_time_wait=self.reconnect_time_wait,
                                       error_cb=self._on_error, disconnected_cb=self._on_disconnected,
                                       reconnected_cb=self._on_reconnected, closed_cb=self._on_closed)
        if self.user:
            options['user'], options['password'] = self.user, self.password
        if self.nats_token:
            options['token'] = self.nats_token
        if self.tls:
            options['tls'] = ssl.create_default_context()
        return options

    async def connect_bus(self):
        self.nc = await nats.connect(**self._connect_options())
        _log.info(f'{self.proxy_name}: Connected to NATS @ {self.nc.connected_url}.')

    async def close_bus(self):
        if self.nc is None or self.nc.is_closed:
            return
        try:
            await self.nc.drain()
        except Exception as e:
            _log.debug(f'{self.proxy_name}: Drain failed ({e!r}); closing.')
            await self.nc.close()

    async def start(self):
        """Connect to NATS first so the proxy only registers with the manager once it can relay."""
        await self.connect_bus()
        try:
            await super().start()
        finally:
            await self.close_bus()

    async def _on_error(self, e: Exception):
        _log.warning(f'{self.proxy_name}: NATS error: {e!r}')

    async def _on_disconnected(self):
        _log.warning(f'{self.proxy_name}: Disconnected from NATS.')

    async def _on_reconnected(self):
        # nats-py re-establishes existing subscriptions itself.
        _log.info(f'{self.proxy_name}: Reconnected to NATS @ {self.nc.connected_url}.')

    async def _on_closed(self):
        _log.info(f'{self.proxy_name}: NATS connection closed.')

    async def on_message(self, msg: Msg):
        """Forward a NATS message to the manager. Must never raise into the nats-py reader."""
        try:
            message = ProtocolProxyMessage(
                method_name='PUBLISH_LOCAL',
                payload=json.dumps({'topic': msg.subject, 'payload': msg.data.hex(), 'headers': msg.headers or {},
                                    'reply': msg.reply or None}).encode('utf8'))
            if not await self.send(self.peers[self.manager], message):
                _log.warning(f'{self.proxy_name}: Unable to forward message on "{msg.subject}" to the manager.')
        except Exception as e:
            _log.warning(f'{self.proxy_name}: Error forwarding message on "{msg.subject}": {e!r}')

    ##################
    # Manager requests
    ##################

    def _decode(self, raw_message: bytes) -> dict | None:
        try:
            message = json.loads(raw_message.decode('utf8'))
        except (UnicodeDecodeError, json.JSONDecodeError) as e:
            _log.warning(f'{self.proxy_name}: Received undecodable request: {e}')
            return None
        if not isinstance(message, dict):
            _log.warning(f'{self.proxy_name}: Expected a JSON object, got {type(message).__name__}.')
            return None
        return message

    @staticmethod
    def _encode_payload(payload: Any) -> bytes:
        if payload is None:
            return b''
        if isinstance(payload, (bytes, bytearray)):
            return bytes(payload)
        if isinstance(payload, str):
            return payload.encode('utf8')
        return json.dumps(payload).encode('utf8')

    def _parse_subjects(self, message: dict) -> list[str]:
        topics = message.get('topics') or ([message['topic']] if message.get('topic') else [])
        if isinstance(topics, str):
            topics = [topics]
        subjects = []
        for entry in topics:
            if isinstance(entry, str) and entry:
                subjects.append(entry)
            elif isinstance(entry, (list, tuple)) and entry and isinstance(entry[0], str):
                subjects.append(entry[0])    # [subject, qos] form used by other buses; NATS has no QoS.
            elif isinstance(entry, dict) and entry.get('topic'):
                subjects.append(str(entry['topic']))
            else:
                _log.warning(f'{self.proxy_name}: Ignoring malformed subject entry: {entry!r}')
        return subjects

    @async_callback
    async def handle_publish_remote(self, headers: ProtocolHeaders, raw_message: bytes):
        if (message := self._decode(raw_message)) is None:
            return
        if not (subject := message.get('topic')):
            _log.warning(f'{self.proxy_name}: PUBLISH_REMOTE without a topic: {message}')
            return
        nats_headers = message.get('headers')
        if nats_headers is not None and not (isinstance(nats_headers, dict)
                                             and all(isinstance(k, str) and isinstance(v, str)
                                                     for k, v in nats_headers.items())):
            _log.warning(f'{self.proxy_name}: Ignoring non string-to-string headers on "{subject}".')
            nats_headers = None
        try:
            await self.nc.publish(subject, self._encode_payload(message.get('payload')), headers=nats_headers)
        except (NATSError, OSError) as e:
            _log.warning(f'{self.proxy_name}: Publish to "{subject}" failed: {e!r}')

    @async_callback
    async def handle_subscribe_remote(self, headers: ProtocolHeaders, raw_message: bytes):
        if (message := self._decode(raw_message)) is None:
            return
        for subject in self._parse_subjects(message):
            if subject in self.subscriptions:
                continue
            try:
                self.subscriptions[subject] = await self.nc.subscribe(subject, cb=self.on_message)
            except (NATSError, OSError, ValueError) as e:
                _log.warning(f'{self.proxy_name}: Subscribe to "{subject}" failed: {e!r}')

    @async_callback
    async def handle_unsubscribe_remote(self, headers: ProtocolHeaders, raw_message: bytes):
        if (message := self._decode(raw_message)) is None:
            return
        for subject in self._parse_subjects(message):
            if (subscription := self.subscriptions.pop(subject, None)) is None:
                continue
            try:
                await subscription.unsubscribe()
            except (NATSError, OSError) as e:
                _log.warning(f'{self.proxy_name}: Unsubscribe from "{subject}" failed: {e!r}')


async def run_proxy(**kwargs) -> int:
    _log.info(f'Launching NATS Proxy using parameters: {redact(kwargs)}.')
    proxy = NATSProxy(**kwargs)    # Must be created inside the running event loop.
    await proxy.start()
    return 0


def launch_nats(parser: ArgumentParser) -> tuple[ArgumentParser, Callable]:
    parser.add_argument('--servers', type=str, default=DEFAULT_SERVER,
                        help='Comma-separated NATS server URLs, e.g. nats://host1:4222,nats://host2:4222.')
    parser.add_argument('--name', type=str, default=None, help='Client name reported to the NATS server.')
    parser.add_argument('--user', type=str, default=None, help='Username for NATS authentication.')
    parser.add_argument('--password', type=str, default=None, help='Password for NATS authentication.')
    parser.add_argument('--nats-token', type=str, default=None, help='Token for NATS authentication.')
    parser.add_argument('--connect-timeout', type=float, default=2.0, help='Connection timeout in seconds.')
    parser.add_argument('--max-reconnect-attempts', type=int, default=-1,
                        help='Reconnection attempts before giving up (-1 for unlimited).')
    parser.add_argument('--reconnect-time-wait', type=float, default=2.0,
                        help='Seconds to wait between reconnection attempts.')
    parser.add_argument('--tls', type=str2bool, default=False,
                        help='Use TLS with the system CA certificates (true/false).')
    return parser, run_proxy


if __name__ == '__main__':
    sys.exit(launch(launch_nats))
