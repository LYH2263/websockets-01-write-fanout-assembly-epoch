"""
:mod:`websockets.fanout` defines fanout epochs for host applications.

A :class:`Fanout` binds a set of connections together with the sans-IO
:class:`~websockets.protocol.Protocol` ledgers and the assemblers of those
connections. It lets a host application:

* fan out a message immediately, like
  :func:`~websockets.asyncio.server.broadcast`;
* *stage* fanout intents in the current epoch and flush them later;
* *advance* to a new epoch: staged intents of the previous epoch are discarded
  and can never be written; assemblies in progress on member connections are
  settled against the new epoch;
* nest fanout scopes: discarding an inner scope never touches the ledgers or
  the assembler queues of an outer scope.

The class itself is independent of the I/O integration. The asyncio and
threading integrations provide subclasses that dispatch writes with their
respective scheduling strategy: synchronously on the event loop for asyncio,
and through a :class:`~concurrent.futures.ThreadPoolExecutor` for threading.

"""

from __future__ import annotations

import traceback
from types import TracebackType
from typing import Any

from .exceptions import ConcurrencyError
from .typing import BytesLike, DataLike


__all__ = ["Fanout"]


class Fanout:
    """
    Fan out messages to a set of connections, organized in epochs.

    Args:
        connections: Connections to which messages are fanned out.
        text: Force sending in Text_ or Binary_ frames, like
            :func:`~websockets.asyncio.server.broadcast`.

    .. _Text: https://datatracker.ietf.org/doc/html/rfc6455#section-5.6

    A :class:`Fanout` is a context manager. It registers a fanout scope on
    every member connection on entry and unregisters it on exit, discarding
    staged intents. Scopes nest: an inner fanout over a subset of the outer
    connections keeps independent ledgers.

    """

    def __init__(
        self,
        connections: Any,
        *,
        text: bool | None = None,
    ) -> None:
        self.connections = list(connections)
        self.text = text
        # id(self) is unique for the lifetime of the fanout, which is also
        # how long the scope is registered on member connections.
        self.token = id(self)
        self.epoch = 0
        self.closed = False
        for connection in self.connections:
            connection.fanout_bind(self)

    # Scope lifecycle.

    def __enter__(self) -> Fanout:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()

    def close(self) -> None:
        """
        Unregister the fanout scope from member connections.

        Staged intents of every epoch are discarded. The assembler queues of
        member connections are untouched. Idempotent.

        """
        if self.closed:
            return
        self.closed = True
        for connection in self.connections:
            connection.fanout_unbind(self)

    # Fanout operations.

    def stage(self, message: DataLike) -> None:
        """
        Stage a fanout intent in the current epoch.

        Nothing is written until :meth:`flush` is called. If the epoch is
        advanced before flushing, the staged intent is discarded.

        Raises:
            TypeError: If ``message`` doesn't have a supported type.

        """
        send, data = self.encode(message)
        for connection in self.connections:
            connection.fanout_stage(self, send, data)

    def flush(self, raise_exceptions: bool = False) -> None:
        """
        Write intents staged in the current epoch.

        Intents staged in previous epochs were already discarded. Connections
        that stopped being open silently skip the flush; connections blocked in
        a fragmented send keep the intent staged until the fragmented message
        ends or the epoch advances.

        """
        exceptions: list[Exception] = []

        def operation(connection: Any) -> None:
            try:
                connection.fanout_flush(self)
            except Exception as exc:
                self.skip(connection, exc, raise_exceptions, exceptions)

        self.fanout_each(operation)

        if raise_exceptions and exceptions:
            raise ExceptionGroup("skipped fanout", exceptions)

    def send(
        self,
        message: DataLike,
        *,
        raise_exceptions: bool = False,
    ) -> None:
        """
        Fan out a message immediately in the current epoch.

        This is equivalent to staging followed by flushing, except that it
        preserves the skipping behavior of
        :func:`~websockets.asyncio.server.broadcast`: connections that aren't
        open are skipped silently and connections sending a fragmented message
        are skipped with a warning, or reported as
        :exc:`~websockets.exceptions.ConcurrencyError` sub-exceptions.

        """
        send, data = self.encode(message)
        exceptions: list[Exception] = []

        def operation(connection: Any) -> None:
            try:
                connection.fanout_deliver(self, send, data)
            except Exception as exc:
                self.skip(connection, exc, raise_exceptions, exceptions)

        self.fanout_each(operation)

        if raise_exceptions and exceptions:
            raise ExceptionGroup("skipped fanout", exceptions)

    def advance(self) -> int:
        """
        Advance member connections to a new epoch.

        Staged intents of the previous epoch are discarded on every member.
        A :meth:`~websockets.asyncio.connection.Connection.recv` in progress
        on a member is aborted with
        :exc:`~websockets.exceptions.EpochAborted`; it may be retried in the
        new epoch. Returns the new epoch number.

        """
        self.epoch += 1
        for connection in self.connections:
            connection.fanout_advance(self)
        return self.epoch

    # Hooks for I/O integrations.

    def fanout_each(self, operation: Any) -> None:
        """
        Run ``operation`` once per member connection.

        The asyncio subclass runs operations sequentially on the event loop
        thread; the threading subclass dispatches them to a thread pool, like
        :func:`~websockets.sync.server.broadcast`.

        """
        for connection in self.connections:
            operation(connection)

    def encode(self, message: DataLike) -> tuple[str, bytes]:
        """Encode a message as a frame selector and bytes payload."""
        if isinstance(message, str):
            return (
                "send_binary" if self.text is False else "send_text",
                message.encode(),
            )
        elif isinstance(message, BytesLike):
            return "send_text" if self.text is True else "send_binary", bytes(message)
        else:
            raise TypeError("data must be str or bytes")

    def skip(
        self,
        connection: Any,
        exc: Exception,
        raise_exceptions: bool,
        exceptions: list[Exception],
    ) -> None:
        """Record or log a failed fanout write, mirroring broadcast()."""
        if isinstance(exc, ConcurrencyError):
            detail = "sending a fragmented message"
        else:
            detail = (
                "failed to write message: "
                + traceback.format_exception_only(exc)[0].strip()
            )
            error = RuntimeError("failed to write message")
            error.__cause__ = exc
            exc = error
        if raise_exceptions:
            exceptions.append(exc)
        else:
            connection.logger.warning("skipped fanout: %s", detail)
