import contextlib
import time
import unittest
import unittest.mock

from websockets.exceptions import ConcurrencyError, EpochAborted, TooManyFragments
from websockets.frames import BINARY, CONT, TEXT, Frame
from websockets.sync import messages
from websockets.sync.messages import *

from ..utils import MS
from .utils import ThreadTestCase


@contextlib.contextmanager
def few_fragments():
    max_fragments = messages.MAX_FRAGMENTS
    messages.MAX_FRAGMENTS = 2
    try:
        yield
    finally:
        messages.MAX_FRAGMENTS = max_fragments


class AssemblerTests(ThreadTestCase):
    def setUp(self):
        self.pause = unittest.mock.Mock()
        self.resume = unittest.mock.Mock()
        self.assembler = Assembler(high=2, low=1, pause=self.pause, resume=self.resume)

    # Test get.

    def test_get_text_message_already_received(self):
        """get returns a text message that is already received."""
        self.assembler.put(Frame(TEXT, b"caf\xc3\xa9"))
        message = self.assembler.get()
        self.assertEqual(message, "café")

    def test_get_binary_message_already_received(self):
        """get returns a binary message that is already received."""
        self.assembler.put(Frame(BINARY, b"tea"))
        message = self.assembler.get()
        self.assertEqual(message, b"tea")

    def test_get_text_message_not_received_yet(self):
        """get returns a text message when it is received."""
        message = None

        def getter():
            nonlocal message
            message = self.assembler.get()

        with self.run_in_thread(getter):
            self.assembler.put(Frame(TEXT, b"caf\xc3\xa9"))
        self.assertEqual(message, "café")

    def test_get_binary_message_not_received_yet(self):
        """get returns a binary message when it is received."""
        message = None

        def getter():
            nonlocal message
            message = self.assembler.get()

        with self.run_in_thread(getter):
            self.assembler.put(Frame(BINARY, b"tea"))
        self.assertEqual(message, b"tea")

    def test_get_fragmented_text_message_already_received(self):
        """get reassembles a fragmented a text message that is already received."""
        self.assembler.put(Frame(TEXT, b"ca", fin=False))
        self.assembler.put(Frame(CONT, b"f\xc3", fin=False))
        self.assembler.put(Frame(CONT, b"\xa9"))
        message = self.assembler.get()
        self.assertEqual(message, "café")

    def test_get_fragmented_binary_message_already_received(self):
        """get reassembles a fragmented binary message that is already received."""
        self.assembler.put(Frame(BINARY, b"t", fin=False))
        self.assembler.put(Frame(CONT, b"e", fin=False))
        self.assembler.put(Frame(CONT, b"a"))
        message = self.assembler.get()
        self.assertEqual(message, b"tea")

    def test_get_fragmented_text_message_not_received_yet(self):
        """get reassembles a fragmented text message when it is received."""
        message = None

        def getter():
            nonlocal message
            message = self.assembler.get()

        with self.run_in_thread(getter):
            self.assembler.put(Frame(TEXT, b"ca", fin=False))
            self.assembler.put(Frame(CONT, b"f\xc3", fin=False))
            self.assembler.put(Frame(CONT, b"\xa9"))
        self.assertEqual(message, "café")

    def test_get_fragmented_binary_message_not_received_yet(self):
        """get reassembles a fragmented binary message when it is received."""
        message = None

        def getter():
            nonlocal message
            message = self.assembler.get()

        with self.run_in_thread(getter):
            self.assembler.put(Frame(BINARY, b"t", fin=False))
            self.assembler.put(Frame(CONT, b"e", fin=False))
            self.assembler.put(Frame(CONT, b"a"))
        self.assertEqual(message, b"tea")

    def test_get_fragmented_text_message_being_received(self):
        """get reassembles a fragmented text message that is partially received."""
        message = None

        def getter():
            nonlocal message
            message = self.assembler.get()

        self.assembler.put(Frame(TEXT, b"ca", fin=False))
        with self.run_in_thread(getter):
            self.assembler.put(Frame(CONT, b"f\xc3", fin=False))
            self.assembler.put(Frame(CONT, b"\xa9"))
        self.assertEqual(message, "café")

    def test_get_fragmented_binary_message_being_received(self):
        """get reassembles a fragmented binary message that is partially received."""
        message = None

        def getter():
            nonlocal message
            message = self.assembler.get()

        self.assembler.put(Frame(BINARY, b"t", fin=False))
        with self.run_in_thread(getter):
            self.assembler.put(Frame(CONT, b"e", fin=False))
            self.assembler.put(Frame(CONT, b"a"))
        self.assertEqual(message, b"tea")

    @few_fragments()
    def test_get_too_many_fragments(self):
        """get rejects a fragmented message with too many frames."""
        self.assembler.put(Frame(BINARY, b"t", fin=False))
        self.assembler.put(Frame(CONT, b"e", fin=False))
        self.assembler.put(Frame(CONT, b"a", fin=True))
        with self.assertRaises(TooManyFragments):
            self.assembler.get()

    def test_get_encoded_text_message(self):
        """get returns a text message without UTF-8 decoding."""
        self.assembler.put(Frame(TEXT, b"caf\xc3\xa9"))
        message = self.assembler.get(decode=False)
        self.assertEqual(message, b"caf\xc3\xa9")

    def test_get_decoded_binary_message(self):
        """get returns a binary message with UTF-8 decoding."""
        self.assembler.put(Frame(BINARY, b"tea"))
        message = self.assembler.get(decode=True)
        self.assertEqual(message, "tea")

    def test_get_resumes_reading(self):
        """get resumes reading when queue goes below the low-water mark."""
        self.assembler.put(Frame(TEXT, b"caf\xc3\xa9"))
        self.assembler.put(Frame(TEXT, b"more caf\xc3\xa9"))
        self.assembler.put(Frame(TEXT, b"water"))

        # queue is above the low-water mark
        self.assembler.get()
        self.resume.assert_not_called()
        # queue is at the low-water mark
        self.assembler.get()
        self.resume.assert_called_once_with()
        # queue is below the low-water mark
        self.assembler.get()
        self.resume.assert_called_once_with()

    def test_get_does_not_resume_reading(self):
        """get does not resume reading when the low-water mark is unset."""
        self.assembler.low = None

        self.assembler.put(Frame(TEXT, b"caf\xc3\xa9"))
        self.assembler.put(Frame(TEXT, b"more caf\xc3\xa9"))
        self.assembler.put(Frame(TEXT, b"water"))
        self.assembler.get()
        self.assembler.get()
        self.assembler.get()
        self.resume.assert_not_called()

    def test_get_timeout_before_first_frame(self):
        """get times out before reading the first frame."""
        with self.assertRaises(TimeoutError):
            self.assembler.get(timeout=MS)

        self.assembler.put(Frame(TEXT, b"caf\xc3\xa9"))
        message = self.assembler.get()
        self.assertEqual(message, "café")

    def test_get_timeout_after_first_frame(self):
        """get times out after reading the first frame."""
        self.assembler.put(Frame(TEXT, b"ca", fin=False))

        with self.assertRaises(TimeoutError):
            self.assembler.get(timeout=MS)

        self.assembler.put(Frame(CONT, b"f\xc3", fin=False))
        self.assembler.put(Frame(CONT, b"\xa9"))
        message = self.assembler.get()
        self.assertEqual(message, "café")

    def test_get_timeout_0_message_already_received(self):
        """get(timeout=0) returns a message that is already received."""
        self.assembler.put(Frame(TEXT, b"caf\xc3\xa9"))
        message = self.assembler.get(timeout=0)
        self.assertEqual(message, "café")

    def test_get_timeout_0_message_not_received_yet(self):
        """get(timeout=0) times out when no message is already received."""
        with self.assertRaises(TimeoutError):
            self.assembler.get(timeout=0)

    def test_get_timeout_0_fragmented_message_already_received(self):
        """get(timeout=0) returns a fragmented message that is already received."""
        self.assembler.put(Frame(TEXT, b"ca", fin=False))
        self.assembler.put(Frame(CONT, b"f\xc3", fin=False))
        self.assembler.put(Frame(CONT, b"\xa9"))
        message = self.assembler.get(timeout=0)
        self.assertEqual(message, "café")

    def test_get_timeout_0_fragmented_message_partially_received(self):
        """get(timeout=0) times out when a fragmented message is partially received."""
        self.assembler.put(Frame(TEXT, b"ca", fin=False))
        self.assembler.put(Frame(CONT, b"f\xc3", fin=False))
        with self.assertRaises(TimeoutError):
            self.assembler.get(timeout=0)

    # Test get_iter.

    def test_get_iter_text_message_already_received(self):
        """get_iter yields a text message that is already received."""
        self.assembler.put(Frame(TEXT, b"caf\xc3\xa9"))
        fragments = list(self.assembler.get_iter())
        self.assertEqual(fragments, ["café"])

    def test_get_iter_binary_message_already_received(self):
        """get_iter yields a binary message that is already received."""
        self.assembler.put(Frame(BINARY, b"tea"))
        fragments = list(self.assembler.get_iter())
        self.assertEqual(fragments, [b"tea"])

    def test_get_iter_text_message_not_received_yet(self):
        """get_iter yields a text message when it is received."""
        fragments = None

        def getter():
            nonlocal fragments
            fragments = list(self.assembler.get_iter())

        with self.run_in_thread(getter):
            self.assembler.put(Frame(TEXT, b"caf\xc3\xa9"))
        self.assertEqual(fragments, ["café"])

    def test_get_iter_binary_message_not_received_yet(self):
        """get_iter yields a binary message when it is received."""
        fragments = None

        def getter():
            nonlocal fragments
            fragments = list(self.assembler.get_iter())

        with self.run_in_thread(getter):
            self.assembler.put(Frame(BINARY, b"tea"))
        self.assertEqual(fragments, [b"tea"])

    def test_get_iter_fragmented_text_message_already_received(self):
        """get_iter yields a fragmented text message that is already received."""
        self.assembler.put(Frame(TEXT, b"ca", fin=False))
        self.assembler.put(Frame(CONT, b"f\xc3", fin=False))
        self.assembler.put(Frame(CONT, b"\xa9"))
        fragments = list(self.assembler.get_iter())
        self.assertEqual(fragments, ["ca", "f", "é"])

    def test_get_iter_fragmented_binary_message_already_received(self):
        """get_iter yields a fragmented binary message that is already received."""
        self.assembler.put(Frame(BINARY, b"t", fin=False))
        self.assembler.put(Frame(CONT, b"e", fin=False))
        self.assembler.put(Frame(CONT, b"a"))
        fragments = list(self.assembler.get_iter())
        self.assertEqual(fragments, [b"t", b"e", b"a"])

    def test_get_iter_fragmented_text_message_not_received_yet(self):
        """get_iter yields a fragmented text message when it is received."""
        iterator = self.assembler.get_iter()
        with contextlib.closing(iterator):
            self.assembler.put(Frame(TEXT, b"ca", fin=False))
            self.assertEqual(next(iterator), "ca")
            self.assembler.put(Frame(CONT, b"f\xc3", fin=False))
            self.assertEqual(next(iterator), "f")
            self.assembler.put(Frame(CONT, b"\xa9"))
            self.assertEqual(next(iterator), "é")

    def test_get_iter_fragmented_binary_message_not_received_yet(self):
        """get_iter yields a fragmented binary message when it is received."""
        iterator = self.assembler.get_iter()
        with contextlib.closing(iterator):
            self.assembler.put(Frame(BINARY, b"t", fin=False))
            self.assertEqual(next(iterator), b"t")
            self.assembler.put(Frame(CONT, b"e", fin=False))
            self.assertEqual(next(iterator), b"e")
            self.assembler.put(Frame(CONT, b"a"))
            self.assertEqual(next(iterator), b"a")

    def test_get_iter_fragmented_text_message_being_received(self):
        """get_iter yields a fragmented text message that is partially received."""
        self.assembler.put(Frame(TEXT, b"ca", fin=False))
        iterator = self.assembler.get_iter()
        with contextlib.closing(iterator):
            self.assertEqual(next(iterator), "ca")
            self.assembler.put(Frame(CONT, b"f\xc3", fin=False))
            self.assertEqual(next(iterator), "f")
            self.assembler.put(Frame(CONT, b"\xa9"))
            self.assertEqual(next(iterator), "é")

    def test_get_iter_fragmented_binary_message_being_received(self):
        """get_iter yields a fragmented binary message that is partially received."""
        self.assembler.put(Frame(BINARY, b"t", fin=False))
        iterator = self.assembler.get_iter()
        with contextlib.closing(iterator):
            self.assertEqual(next(iterator), b"t")
            self.assembler.put(Frame(CONT, b"e", fin=False))
            self.assertEqual(next(iterator), b"e")
            self.assembler.put(Frame(CONT, b"a"))
            self.assertEqual(next(iterator), b"a")

    def test_get_iter_encoded_text_message(self):
        """get_iter yields a text message without UTF-8 decoding."""
        self.assembler.put(Frame(TEXT, b"ca", fin=False))
        self.assembler.put(Frame(CONT, b"f\xc3", fin=False))
        self.assembler.put(Frame(CONT, b"\xa9"))
        fragments = list(self.assembler.get_iter(decode=False))
        self.assertEqual(fragments, [b"ca", b"f\xc3", b"\xa9"])

    def test_get_iter_decoded_binary_message(self):
        """get_iter yields a binary message with UTF-8 decoding."""
        self.assembler.put(Frame(BINARY, b"t", fin=False))
        self.assembler.put(Frame(CONT, b"e", fin=False))
        self.assembler.put(Frame(CONT, b"a"))
        fragments = list(self.assembler.get_iter(decode=True))
        self.assertEqual(fragments, ["t", "e", "a"])

    def test_get_iter_resumes_reading(self):
        """get_iter resumes reading when queue goes below the low-water mark."""
        self.assembler.put(Frame(BINARY, b"t", fin=False))
        self.assembler.put(Frame(CONT, b"e", fin=False))
        self.assembler.put(Frame(CONT, b"a"))

        iterator = self.assembler.get_iter()
        with contextlib.closing(iterator):
            # queue is above the low-water mark
            next(iterator)
            self.resume.assert_not_called()
            # queue is at the low-water mark
            next(iterator)
            self.resume.assert_called_once_with()
            # queue is below the low-water mark
            next(iterator)
            self.resume.assert_called_once_with()

    def test_get_iter_does_not_resume_reading(self):
        """get_iter does not resume reading when the low-water mark is unset."""
        self.assembler.low = None

        self.assembler.put(Frame(BINARY, b"t", fin=False))
        self.assembler.put(Frame(CONT, b"e", fin=False))
        self.assembler.put(Frame(CONT, b"a"))
        iterator = self.assembler.get_iter()
        with contextlib.closing(iterator):
            next(iterator)
            next(iterator)
            next(iterator)
            self.resume.assert_not_called()

    # Test put.

    def test_put_pauses_reading(self):
        """put pauses reading when queue goes above the high-water mark."""
        # queue is below the high-water mark
        self.assembler.put(Frame(TEXT, b"caf\xc3\xa9"))
        self.assembler.put(Frame(BINARY, b"t", fin=False))
        self.pause.assert_not_called()
        # queue is at the high-water mark
        self.assembler.put(Frame(CONT, b"e", fin=False))
        self.pause.assert_called_once_with()
        # queue is above the high-water mark
        self.assembler.put(Frame(CONT, b"a"))
        self.pause.assert_called_once_with()

    def test_put_does_not_pause_reading(self):
        """put does not pause reading when the high-water mark is unset."""
        self.assembler.high = None

        self.assembler.put(Frame(TEXT, b"caf\xc3\xa9"))
        self.assembler.put(Frame(BINARY, b"t", fin=False))
        self.assembler.put(Frame(CONT, b"e", fin=False))
        self.assembler.put(Frame(CONT, b"a"))
        self.pause.assert_not_called()

    # Test assembly epochs.

    def test_get_aborted_when_epoch_advances(self):
        """reset_epoch aborts a get waiting for a frame with EpochAborted."""

        def advance():
            time.sleep(2 * MS)
            self.assembler.reset_epoch()

        with self.run_in_thread(advance):
            with self.assertRaises(EpochAborted):
                self.assembler.get(timeout=1)

        # The receive is recoverable in the new epoch.
        self.assembler.put(Frame(TEXT, b"new"), epoch=1)
        message = self.assembler.get()
        self.assertEqual(message, "new")

    def test_get_aborted_mid_fragmented_message(self):
        """reset_epoch discards fragments already received in the old epoch."""
        self.assembler.put(Frame(TEXT, b"ca", fin=False))

        def advance():
            time.sleep(2 * MS)
            self.assembler.reset_epoch()

        with self.run_in_thread(advance):
            with self.assertRaises(EpochAborted):
                self.assembler.get(timeout=1)

        # Neither the old fragment nor an old continuation can be assembled.
        self.assembler.put(Frame(CONT, b"fe"), epoch=0)
        self.assembler.put(Frame(TEXT, b"new"), epoch=1)
        message = self.assembler.get()
        self.assertEqual(message, "new")

    def test_stale_queued_frames_are_discarded(self):
        """Frames queued before an epoch boundary are discarded lazily."""
        self.assembler.put(Frame(TEXT, b"old"))
        # No get is in progress; reset_epoch only bumps the epoch.
        self.assembler.reset_epoch()
        self.assembler.put(Frame(TEXT, b"new"), epoch=1)
        message = self.assembler.get()
        self.assertEqual(message, "new")

    def test_get_iter_aborted_before_first_frame(self):
        """reset_epoch aborts get_iter waiting for its first frame."""

        def advance():
            time.sleep(2 * MS)
            self.assembler.reset_epoch()

        with self.run_in_thread(advance):
            with self.assertRaises(EpochAborted):
                for _ in self.assembler.get_iter():
                    self.fail("no fragment expected")

        # The receive is recoverable in the new epoch.
        self.assembler.put(Frame(TEXT, b"new"), epoch=1)
        fragments = list(self.assembler.get_iter())
        self.assertEqual(fragments, ["new"])

    def test_get_iter_aborted_between_fragments(self):
        """reset_epoch aborts get_iter waiting for a continuation frame."""
        self.assembler.put(Frame(TEXT, b"ca", fin=False))
        iterator = self.assembler.get_iter()
        self.assertEqual(next(iterator), "ca")

        def advance():
            time.sleep(2 * MS)
            self.assembler.reset_epoch()

        with self.run_in_thread(advance):
            with self.assertRaises(EpochAborted):
                next(iterator)

    def test_reset_epoch_after_close_is_noop(self):
        """reset_epoch doesn't change the epoch after close is called."""
        self.assembler.close()
        self.assembler.reset_epoch()
        self.assertEqual(self.assembler.epoch, 0)

    def test_get_after_epoch_abort_and_close_raises_eof(self):
        """A queued epoch abort hides behind close: get raises EOFError."""

        def advance_then_close():
            time.sleep(2 * MS)
            self.assembler.reset_epoch()
            self.assembler.close()

        with self.run_in_thread(advance_then_close):
            with self.assertRaises(EOFError):
                self.assembler.get(timeout=1)

    # Test termination.

    def test_get_fails_when_interrupted_by_close(self):
        """get raises EOFError when close is called."""

        def closer():
            time.sleep(2 * MS)
            self.assembler.close()

        with self.run_in_thread(closer):
            with self.assertRaises(EOFError):
                self.assembler.get()

    def test_get_iter_fails_when_interrupted_by_close(self):
        """get_iter raises EOFError when close is called."""

        def closer():
            time.sleep(2 * MS)
            self.assembler.close()

        with self.run_in_thread(closer):
            with self.assertRaises(EOFError):
                for _ in self.assembler.get_iter():
                    self.fail("no fragment expected")

    def test_get_fails_after_close(self):
        """get raises EOFError after close is called."""
        self.assembler.close()
        with self.assertRaises(EOFError):
            self.assembler.get()

    def test_get_iter_fails_after_close(self):
        """get_iter raises EOFError after close is called."""
        self.assembler.close()
        with self.assertRaises(EOFError):
            for _ in self.assembler.get_iter():
                self.fail("no fragment expected")

    def test_get_queued_message_after_close(self):
        """get returns a message after close is called."""
        self.assembler.put(Frame(TEXT, b"caf\xc3\xa9"))
        self.assembler.close()
        message = self.assembler.get()
        self.assertEqual(message, "café")

    def test_get_iter_queued_message_after_close(self):
        """get_iter yields a message after close is called."""
        self.assembler.put(Frame(TEXT, b"caf\xc3\xa9"))
        self.assembler.close()
        fragments = list(self.assembler.get_iter())
        self.assertEqual(fragments, ["café"])

    def test_get_queued_fragmented_message_after_close(self):
        """get reassembles a fragmented message after close is called."""
        self.assembler.put(Frame(BINARY, b"t", fin=False))
        self.assembler.put(Frame(CONT, b"e", fin=False))
        self.assembler.put(Frame(CONT, b"a"))
        self.assembler.close()
        self.assembler.close()
        message = self.assembler.get()
        self.assertEqual(message, b"tea")

    def test_get_iter_queued_fragmented_message_after_close(self):
        """get_iter yields a fragmented message after close is called."""
        self.assembler.put(Frame(BINARY, b"t", fin=False))
        self.assembler.put(Frame(CONT, b"e", fin=False))
        self.assembler.put(Frame(CONT, b"a"))
        self.assembler.close()
        fragments = list(self.assembler.get_iter())
        self.assertEqual(fragments, [b"t", b"e", b"a"])

    def test_get_partially_queued_fragmented_message_after_close(self):
        """get raises EOFError on a partial fragmented message after close is called."""
        self.assembler.put(Frame(BINARY, b"t", fin=False))
        self.assembler.put(Frame(CONT, b"e", fin=False))
        self.assembler.close()
        with self.assertRaises(EOFError):
            self.assembler.get()

    def test_get_iter_partially_queued_fragmented_message_after_close(self):
        """get_iter yields a partial fragmented message after close is called."""
        self.assembler.put(Frame(BINARY, b"t", fin=False))
        self.assembler.put(Frame(CONT, b"e", fin=False))
        self.assembler.close()
        fragments = []
        with self.assertRaises(EOFError):
            for fragment in self.assembler.get_iter():
                fragments.append(fragment)
            else:
                self.fail("did not raise")
        self.assertEqual(fragments, [b"t", b"e"])

    def test_put_fails_after_close(self):
        """put raises EOFError after close is called."""
        self.assembler.close()
        with self.assertRaises(EOFError):
            self.assembler.put(Frame(TEXT, b"caf\xc3\xa9"))

    def test_close_resumes_reading(self):
        """close unblocks reading when queue is above the high-water mark."""
        self.assembler.put(Frame(TEXT, b"caf\xc3\xa9"))
        self.assembler.put(Frame(TEXT, b"more caf\xc3\xa9"))
        self.assembler.put(Frame(TEXT, b"water"))
        # queue is at the high-water mark
        assert self.assembler.paused

        self.assembler.close()
        self.resume.assert_called_once_with()

    def test_close_is_idempotent(self):
        """close can be called multiple times safely."""
        self.assembler.close()
        self.assembler.close()

    # Test (non-)concurrency.

    def test_get_fails_when_get_is_running(self):
        """get cannot be called concurrently."""
        with self.run_in_thread(self.assembler.get):
            with self.assertRaises(ConcurrencyError):
                self.assembler.get()
            self.assembler.put(Frame(TEXT, b""))  # unlock other thread

    def test_get_fails_when_get_iter_is_running(self):
        """get cannot be called concurrently with get_iter."""
        with self.run_in_thread(lambda: list(self.assembler.get_iter())):
            with self.assertRaises(ConcurrencyError):
                self.assembler.get()
            self.assembler.put(Frame(TEXT, b""))  # unlock other thread

    def test_get_iter_fails_when_get_is_running(self):
        """get_iter cannot be called concurrently with get."""
        with self.run_in_thread(self.assembler.get):
            with self.assertRaises(ConcurrencyError):
                list(self.assembler.get_iter())
            self.assembler.put(Frame(TEXT, b""))  # unlock other thread

    def test_get_iter_fails_when_get_iter_is_running(self):
        """get_iter cannot be called concurrently."""
        with self.run_in_thread(lambda: list(self.assembler.get_iter())):
            with self.assertRaises(ConcurrencyError):
                list(self.assembler.get_iter())
            self.assembler.put(Frame(TEXT, b""))  # unlock other thread

    # Test setting limits.

    def test_set_high_water_mark(self):
        """high sets the high-water and low-water marks."""
        assembler = Assembler(high=10)
        self.assertEqual(assembler.high, 10)
        self.assertEqual(assembler.low, 2)

    def test_set_low_water_mark(self):
        """low sets the low-water and high-water marks."""
        assembler = Assembler(low=5)
        self.assertEqual(assembler.low, 5)
        self.assertEqual(assembler.high, 20)

    def test_set_high_and_low_water_marks(self):
        """high and low set the high-water and low-water marks."""
        assembler = Assembler(high=10, low=5)
        self.assertEqual(assembler.high, 10)
        self.assertEqual(assembler.low, 5)

    def test_unset_high_and_low_water_marks(self):
        """High-water and low-water marks are unset."""
        assembler = Assembler()
        self.assertEqual(assembler.high, None)
        self.assertEqual(assembler.low, None)

    def test_set_invalid_high_water_mark(self):
        """high must be a non-negative integer."""
        with self.assertRaises(ValueError):
            Assembler(high=-1)

    def test_set_invalid_low_water_mark(self):
        """low must be higher than high."""
        with self.assertRaises(ValueError):
            Assembler(low=10, high=5)
