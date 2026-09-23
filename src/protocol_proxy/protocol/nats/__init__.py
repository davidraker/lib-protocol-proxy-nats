"""NATS plugin for protocol_proxy.

The proxy class is imported lazily so that running ``python -m protocol_proxy.protocol.nats.nats_proxy``
executes the module exactly once, and so that importing this package has no side effects.
"""
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .nats_proxy import NATSProxy

__all__ = ['NATSProxy', 'PROXY_CLASS']


def __getattr__(name: str):
    if name in ('NATSProxy', 'PROXY_CLASS'):
        from .nats_proxy import NATSProxy
        return NATSProxy
    raise AttributeError(f'module {__name__!r} has no attribute {name!r}')
