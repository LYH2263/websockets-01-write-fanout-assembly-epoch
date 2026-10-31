import unittest
import unittest.mock

from websockets.exceptions import ConcurrencyError
from websockets.fanout import Fanout


class FakeConnection:
    """Connection double recording fanout hooks."""

    def __init__(self, *, fail_flush=False, fail_deliver=False):
        self.bound = []
        self.unbound = []
        self.staged = []
        self.flushed = []
        self.advanced = []
        self.delivered = []
        self.fail_flush = fail_flush
        self.fail_deliver = fail_deliver
        self.logger = unittest.mock.Mock()

    def fanout_bind(self, fanout):
        self.bound.append(fanout.token)

    def fanout_unbind(self, fanout):
        self.unbound.append(fanout.token)

    def fanout_stage(self, fanout, send, data):
        self.staged.append((fanout.token, send, data))

    def fanout_flush(self, fanout):
        if self.fail_flush is ConcurrencyError:
            raise ConcurrencyError("sending a fragmented message")
        if self.fail_flush:
            raise OSError("boom")
        self.flushed.append(fanout.token)

    def fanout_advance(self, fanout):
        self.advanced.append(fanout.token)

    def fanout_deliver(self, fanout, send, data):
        if self.fail_deliver is ConcurrencyError:
            raise ConcurrencyError("sending a fragmented message")
        if self.fail_deliver:
            raise OSError("boom")
        self.delivered.append((fanout.token, send, data))


class FanoutTests(unittest.TestCase):
    def test_init_binds_scope_on_every_connection(self):
        first = FakeConnection()
        second = FakeConnection()
        fanout = Fanout([first, second])
        self.assertEqual(first.bound, [fanout.token])
        self.assertEqual(second.bound, [fanout.token])
        self.assertEqual(fanout.epoch, 0)
        self.assertFalse(fanout.closed)

    def test_init_without_connections(self):
        fanout = Fanout([])
        self.assertEqual(fanout.connections, [])
        fanout.close()

    def test_context_manager_unbinds_on_exit(self):
        connection = FakeConnection()
        with Fanout([connection]) as fanout:
            self.assertIsInstance(fanout, Fanout)
        self.assertEqual(connection.unbound, [fanout.token])
        self.assertTrue(fanout.closed)

    def test_context_manager_unbinds_on_exception(self):
        connection = FakeConnection()
        with self.assertRaises(RuntimeError):
            with Fanout([connection]) as fanout:
                raise RuntimeError("boom")
        self.assertEqual(connection.unbound, [fanout.token])

    def test_close_is_idempotent(self):
        connection = FakeConnection()
        fanout = Fanout([connection])
        fanout.close()
        fanout.close()
        self.assertEqual(connection.unbound, [fanout.token])

    def test_stage_stages_text_on_every_connection(self):
        first = FakeConnection()
        second = FakeConnection()
        fanout = Fanout([first, second])
        fanout.stage("café")
        self.assertEqual(first.staged, [(fanout.token, "send_text", "café".encode())])
        self.assertEqual(second.staged, [(fanout.token, "send_text", "café".encode())])

    def test_stage_stages_binary(self):
        connection = FakeConnection()
        fanout = Fanout([connection])
        fanout.stage(b"Spam")
        self.assertEqual(connection.staged, [(fanout.token, "send_binary", b"Spam")])

    def test_stage_with_text_forced_binary(self):
        connection = FakeConnection()
        fanout = Fanout([connection], text=False)
        fanout.stage("Spam")
        self.assertEqual(connection.staged, [(fanout.token, "send_binary", b"Spam")])

    def test_stage_with_text_forced_on_bytes(self):
        connection = FakeConnection()
        fanout = Fanout([connection], text=True)
        fanout.stage(bytearray(b"Spam"))
        self.assertEqual(connection.staged, [(fanout.token, "send_text", b"Spam")])

    def test_stage_rejects_unsupported_type(self):
        fanout = Fanout([FakeConnection()])
        with self.assertRaises(TypeError) as raised:
            fanout.stage(42)
        self.assertEqual(str(raised.exception), "data must be str or bytes")

    def test_flush_flushes_every_connection(self):
        first = FakeConnection()
        second = FakeConnection()
        fanout = Fanout([first, second])
        fanout.stage("Spam")
        fanout.flush()
        self.assertEqual(first.flushed, [fanout.token])
        self.assertEqual(second.flushed, [fanout.token])

    def test_flush_logs_concurrency_error(self):
        connection = FakeConnection(fail_flush=ConcurrencyError)
        fanout = Fanout([connection])
        fanout.flush()
        connection.logger.warning.assert_called_once_with(
            "skipped fanout: %s",
            "sending a fragmented message",
        )

    def test_flush_logs_other_error(self):
        connection = FakeConnection(fail_flush=True)
        fanout = Fanout([connection])
        fanout.flush()
        message = connection.logger.warning.call_args.args[1]
        self.assertIn("failed to write message", message)

    def test_flush_raises_exception_group(self):
        connection = FakeConnection(fail_flush=True)
        fanout = Fanout([connection])
        with self.assertRaises(ExceptionGroup) as raised:
            fanout.flush(raise_exceptions=True)
        self.assertEqual(raised.exception.message, "skipped fanout")
        self.assertIsInstance(raised.exception.exceptions[0], RuntimeError)
        self.assertIsInstance(
            raised.exception.exceptions[0].__cause__,
            OSError,
        )

    def test_flush_raises_concurrency_error_in_exception_group(self):
        connection = FakeConnection()
        connection.fail_deliver = ConcurrencyError
        fanout = Fanout([connection])
        with self.assertRaises(ExceptionGroup) as raised:
            fanout.send("Spam", raise_exceptions=True)
        self.assertIsInstance(raised.exception.exceptions[0], ConcurrencyError)

    def test_send_raises_failed_write_in_exception_group(self):
        connection = FakeConnection(fail_deliver=True)
        fanout = Fanout([connection])
        with self.assertRaises(ExceptionGroup) as raised:
            fanout.send("Spam", raise_exceptions=True)
        self.assertEqual(raised.exception.message, "skipped fanout")
        error = raised.exception.exceptions[0]
        self.assertIsInstance(error, RuntimeError)
        self.assertIsInstance(error.__cause__, OSError)

    def test_flush_with_raise_exceptions_and_no_errors(self):
        fanout = Fanout([FakeConnection()])
        fanout.stage("Spam")
        fanout.flush(raise_exceptions=True)  # no exception raised

    def test_send_delivers_immediately(self):
        first = FakeConnection()
        second = FakeConnection()
        fanout = Fanout([first, second])
        fanout.send("Spam")
        self.assertEqual(first.delivered, [(fanout.token, "send_text", b"Spam")])
        self.assertEqual(second.delivered, [(fanout.token, "send_text", b"Spam")])

    def test_send_logs_skipped_connection(self):
        connection = FakeConnection(fail_deliver=ConcurrencyError)
        fanout = Fanout([connection])
        fanout.send("Spam")
        connection.logger.warning.assert_called_once_with(
            "skipped fanout: %s",
            "sending a fragmented message",
        )
        self.assertEqual(connection.delivered, [])

    def test_advance_advances_every_connection(self):
        first = FakeConnection()
        second = FakeConnection()
        fanout = Fanout([first, second])
        self.assertEqual(fanout.advance(), 1)
        self.assertEqual(first.advanced, [fanout.token])
        self.assertEqual(second.advanced, [fanout.token])
        self.assertEqual(fanout.epoch, 1)
        self.assertEqual(fanout.advance(), 2)

    def test_nested_fanouts_have_independent_scopes(self):
        outer_only = FakeConnection()
        shared = FakeConnection()
        with Fanout([outer_only, shared]) as outer:
            with Fanout([shared]) as inner:
                self.assertNotEqual(outer.token, inner.token)
                self.assertEqual(shared.bound, [outer.token, inner.token])
                inner.stage("inner")
                outer.stage("outer")
                self.assertEqual(
                    shared.staged,
                    [
                        (inner.token, "send_text", b"inner"),
                        (outer.token, "send_text", b"outer"),
                    ],
                )
                inner.advance()
            # Closing the inner fanout doesn't unbind the outer scope.
            self.assertEqual(shared.unbound, [inner.token])
            # The outer staged intent is still there.
            outer.flush()
        self.assertEqual(shared.unbound, [inner.token, outer.token])
        self.assertEqual(shared.flushed, [outer.token])

    def test_fanout_each_runs_operation_in_order(self):
        first = FakeConnection()
        second = FakeConnection()
        fanout = Fanout([first, second])
        seen = []
        fanout.fanout_each(lambda connection: seen.append(connection))
        self.assertEqual(seen, [first, second])
