import asyncio
import json
from types import SimpleNamespace
from unittest import mock
from uuid import uuid4

import pytest

import protocol_proxy.protocol.nats as nats_package
from protocol_proxy.protocol.nats import nats_proxy as module
from protocol_proxy.protocol.nats.nats_proxy import NATSProxy, launch_nats
from protocol_proxy.proxy.launch import proxy_command_parser


def run(coro):
    return asyncio.run(coro)


def make_proxy(**overrides) -> NATSProxy:
    """Construct inside a running loop, as AsyncioIPCConnector requires."""
    manager_id, manager_token = uuid4(), uuid4()
    kwargs = dict(proxy_id=uuid4(), token=uuid4(), proxy_name='test', manager_address='127.0.0.1',
                  manager_port=1, manager_id=manager_id, manager_token=manager_token)
    kwargs.update(overrides)
    p = NATSProxy(**kwargs)
    p.nc = mock.MagicMock()
    p.nc.publish = mock.AsyncMock()
    p.nc.subscribe = mock.AsyncMock(side_effect=lambda subject, cb: mock.MagicMock(unsubscribe=mock.AsyncMock()))
    p.send = mock.AsyncMock(return_value=True)
    p.headers = SimpleNamespace(sender_id=manager_id, sender_token=manager_token)
    return p


async def call(proxy, handler, message):
    raw = message if isinstance(message, bytes) else json.dumps(message).encode('utf8')
    return await handler(proxy, proxy.headers, raw)


def test_module_compiles_and_exposes_proxy_class_lazily():
    assert nats_package.PROXY_CLASS is NATSProxy
    with pytest.raises(AttributeError):
        nats_package.nope


def test_parse_servers():
    assert NATSProxy.parse_servers('nats://a:4222, nats://b:4222,') == ['nats://a:4222', 'nats://b:4222']
    assert NATSProxy.parse_servers(['nats://a']) == ['nats://a']
    with pytest.raises(ValueError):
        NATSProxy.parse_servers('')


def test_construction_and_options():
    async def main():
        p = make_proxy(servers='nats://x:1,nats://y:2', user='u', password='p', nats_token='t', tls=True)
        assert p.servers == ['nats://x:1', 'nats://y:2']
        assert {'PUBLISH_REMOTE', 'SUBSCRIBE_REMOTE', 'UNSUBSCRIBE_REMOTE'} <= set(p.callbacks)
        options = p._connect_options()
        assert options['user'] == 'u' and options['password'] == 'p' and options['token'] == 't'
        assert options['name'] == 'test' and options['tls'] is not None
        assert p.topic_delimiter() == '.'
    run(main())


def test_publish_remote():
    async def main():
        p = make_proxy()
        await call(p, p.handle_publish_remote, {'topic': 'a.b', 'payload': {'v': 1}, 'headers': {'h': 'x'}})
        p.nc.publish.assert_awaited_with('a.b', b'{"v": 1}', headers={'h': 'x'})
        await call(p, p.handle_publish_remote, {'topic': 'a.c', 'payload': 'text', 'headers': {'bad': 1}})
        p.nc.publish.assert_awaited_with('a.c', b'text', headers=None)
        await call(p, p.handle_publish_remote, {'payload': 1})       # no topic
        await call(p, p.handle_publish_remote, b'not json')
        assert p.nc.publish.await_count == 2
    run(main())


def test_publish_remote_rejects_unauthenticated():
    async def main():
        p = make_proxy()
        bad = SimpleNamespace(sender_id=p.headers.sender_id, sender_token=uuid4())
        await p.handle_publish_remote(p, bad, json.dumps({'topic': 't', 'payload': 1}).encode())
        p.nc.publish.assert_not_awaited()
    run(main())


def test_subscribe_and_unsubscribe_remote():
    async def main():
        p = make_proxy()
        await call(p, p.handle_subscribe_remote, {'topics': ['a.>', ['b', 0], {'topic': 'c'}, 7]})
        assert set(p.subscriptions) == {'a.>', 'b', 'c'}
        assert p.nc.subscribe.await_count == 3
        await call(p, p.handle_subscribe_remote, {'topic': 'a.>'})    # duplicate ignored
        assert p.nc.subscribe.await_count == 3
        sub = p.subscriptions['b']
        await call(p, p.handle_unsubscribe_remote, {'topics': ['b', 'zzz']})
        sub.unsubscribe.assert_awaited_once()
        assert set(p.subscriptions) == {'a.>', 'c'}
    run(main())


def test_on_message_forwards_and_never_raises():
    async def main():
        p = make_proxy()
        msg = SimpleNamespace(subject='a.b', data=b'{"x": 1}', headers={'k': 'v'}, reply='')
        await p.on_message(msg)
        peer, message = p.send.await_args.args
        assert peer is p.peers[p.manager]
        body = json.loads(message.payload)
        assert message.method_name == 'PUBLISH_LOCAL'
        assert bytes.fromhex(body['payload']) == b'{"x": 1}' and body['headers'] == {'k': 'v'}
        assert body['reply'] is None
        p.send.side_effect = RuntimeError('boom')
        await p.on_message(msg)    # swallowed
    run(main())


def test_start_connects_before_registering_and_closes_after():
    async def main():
        p = make_proxy()
        order = []
        nc = mock.MagicMock(is_closed=False, connected_url='nats://x', drain=mock.AsyncMock())

        async def fake_connect(**options):
            order.append('connect')
            assert options['servers'] == ['nats://localhost:4222']
            return nc

        async def fake_super_start(self):
            order.append('register')
        with mock.patch.object(module.nats, 'connect', fake_connect), \
             mock.patch.object(module.AsyncioProtocolProxy, 'start', fake_super_start):
            await p.start()
        assert order == ['connect', 'register']
        nc.drain.assert_awaited_once()
    run(main())


def test_start_propagates_connection_failure():
    async def main():
        p = make_proxy()
        with mock.patch.object(module.nats, 'connect', mock.AsyncMock(side_effect=OSError('no server'))), \
             mock.patch.object(module.AsyncioProtocolProxy, 'start', mock.AsyncMock()) as start:
            with pytest.raises(OSError):
                await p.start()
            start.assert_not_awaited()
    run(main())


def test_launch_parser():
    parser, runner = launch_nats(proxy_command_parser())
    opts = parser.parse_args(['--proxy-id', uuid4().hex, '--proxy-name', 'n', '--manager-id', uuid4().hex,
                              '--servers', 'nats://a:1,nats://b:2', '--tls', 'true', '--nats-token', 'tok'])
    assert opts.servers == 'nats://a:1,nats://b:2' and opts.tls is True and opts.nats_token == 'tok'
    assert NATSProxy.parse_servers(opts.servers) == ['nats://a:1', 'nats://b:2']
    assert asyncio.iscoroutinefunction(runner)
