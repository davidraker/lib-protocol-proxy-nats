"""NATS plugin for protocol_proxy.

Proxies are launched with ``python -m protocol_proxy.proxy <module>:<class>`` (see protocol_proxy.proxy.launch), so
this package may import its proxy class directly.
"""
from .nats_proxy import NATSProxy, launch_nats, run_proxy

__all__ = ['NATSProxy', 'PROXY_CLASS', 'launch_nats', 'run_proxy']

PROXY_CLASS = NATSProxy
