import asyncio
import json
import logging
import sys

from argparse import ArgumentParser
from nats import connect
from nats.aio.msg import Msg
from nats.errors import ConnectionClosedError, TimeoutError, NoServersError
from typing import Type

from protocol_proxy.ipc import callback, ProtocolHeaders, ProtocolProxyMessage
from protocol_proxy.proxy import launch
from protocol_proxy.proxy.asyncio import AsyncioProtocolProxy

logging.basicConfig(filename='protoproxy.log', level=logging.DEBUG,
                    format='%(asctime)s - %(message)s')
_log = logging.getLogger(__name__)


class NATSProxy(AsyncioProtocolProxy):
    # TODO: Probbably shouldn't really have the demo as a default. This is to test in development.
    def __init__(self, servers: str | list[str], bus_kwargs=None, **kwargs):
        _log.debug('IN NATSPROXY __init__')
        super().__init__(**kwargs)
        bus_kwargs = bus_kwargs or {}
        # TODO: Implement TLS in BusAdapter (should be able to be passed to bus_kwargs here).
        # TODO: closed_cb, disconnected_cb, error_cb reconnected_cb kwargs to connect can be used to manager the ready state.
        self.nc = await connect(servers=servers, **bus_kwargs)
        _log.debug(f'{self.proxy_name} IN NATS PROXY AFTER CONNECT')
        self.subscribed_topics: list[str | tuple[str, int]] = []

        self.register_callback(self.handle_publish_remote, 'PUBLISH_REMOTE')
        self.register_callback(self.handle_subscribe_remote, 'SUBSCRIBE_REMOTE')

        for subject in self.subscribed_topics:
            self.nc.subscribe(subject, cb=self.on_message)

    @staticmethod
    def topic_delimiter() -> str:
        return '.'

    async def on_message(self, msg: Msg):
        """The callback for when a PUBLISH message is received from the NATS server."""
        message = ProtocolProxyMessage(
            method_name='PUBLISH_LOCAL',
            payload=json.dumps({
                'topic': msg.subject,
                'payload': msg.data.hex(),
                'headers': msg.headers or {}
            }).encode('utf8')
        )
        await self.send(self.peers[self.manager].socket_params, message)

    @classmethod
    def get_unique_remote_id(cls, unique_remote_id: tuple) -> tuple:
        return unique_remote_id

    @callback
    def handle_publish_remote(self, headers: ProtocolHeaders, raw_message: bytes):
        _log.debug(f'NATSProxy {self.proxy_name}: IN HANDLE PUBLISH REMOTE!')

    @callback
    def handle_subscribe_remote(self, headers: ProtocolHeaders, raw_message: bytes):
        _log.debug(f'NATSProxy {self.proxy_name}: IN HANDLE SUBSCRIBE REMOTE!')
        message = json.loads(raw_message.decode('utf8'))
        self.subscribed_topics.extend(message.get('topics'))
        for subject in message.get('topics'):
            self.nc.subscribe(subject, cb=self.on_message)


async def run_proxy(servers, **kwargs):
    # TODO: Need to decide how to really handle "bus_kwargs".
    #  Probably just add "nats_" in front of any conflicts?
    mp = NATSProxy(servers, **kwargs)
    await mp.start()

def launch_mqtt(parser: ArgumentParser) -> (ArgumentParser, Type[AsyncioProtocolProxy]):
    # _log.debug(f'IN LAUNCH NATS')
    parser.add_argument('--servers', type=list[str], default=["nats://demo.nats.io:4222"],
                        help='Address of the MQTT broker.')
    # TODO: Add the options which can be passed to connect:
    #  These might be passed as bus_kwargs or separated in final version.
    #  They can be found here:
    #  https://nats-io.github.io/nats.py/modules.html#nats.aio.client.Client.connect
    return parser, run_proxy

if __name__ == '__main__':
    sys.exit(launch(launch_mqtt))