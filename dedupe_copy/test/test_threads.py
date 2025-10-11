import unittest
import queue
import threading
import time
from unittest.mock import MagicMock, patch
from dedupe_copy.threads import (
    DeleteThread,
    ProgressThread,
    ReadThread,
    ResultProcessor,
    WalkThread,
    _is_file_processing_required,
)
from dedupe_copy.config import WalkConfig


class TestIsFileProcessingRequired(unittest.TestCase):
    def test_ignored_with_progress_queue(self):
        filepath = "/tmp/some/file.txt"
        already_processed = set()
        ignore = ["*.txt"]
        extensions = None
        progress_queue = queue.PriorityQueue()

        result = _is_file_processing_required(
            filepath, already_processed, ignore, extensions, progress_queue
        )

        self.assertFalse(result)
        self.assertFalse(progress_queue.empty())
        item = progress_queue.get()
        self.assertEqual(item[1], "ignored")


class TestResultProcessor(unittest.TestCase):
    def setUp(self):
        # Monkey patch the incremental save size to speed up tests
        self.original_save_size = ResultProcessor.INCREMENTAL_SAVE_SIZE
        ResultProcessor.INCREMENTAL_SAVE_SIZE = 2

    def tearDown(self):
        # Restore original value
        ResultProcessor.INCREMENTAL_SAVE_SIZE = self.original_save_size

    def test_result_processor_handles_processing_error(self):
        stop_event = threading.Event()
        result_queue = queue.Queue()
        progress_queue = queue.PriorityQueue()
        collisions = MagicMock()
        manifest = MagicMock()

        # This will cause a TypeError when the manifest is accessed
        manifest.__getitem__.side_effect = TypeError("mocked type error")

        # Put a validly structured item in the queue
        result_queue.put(("some_md5", 100, 123.45, "a/file/path"))

        processor = ResultProcessor(
            stop_event,
            result_queue,
            collisions,
            manifest,
            progress_queue=progress_queue,
        )
        processor.start()

        # Give the thread time to process the item and wait for it to finish
        result_queue.join()
        stop_event.set()
        processor.join(timeout=2)

        self.assertFalse(
            processor.is_alive(), "Processor thread should have terminated"
        )

        # Check that an error was logged to the progress queue
        self.assertFalse(progress_queue.empty())
        _priority, msg_type, path, error_msg = progress_queue.get()
        self.assertEqual(msg_type, "error")
        self.assertEqual(path, "a/file/path")
        self.assertIn("ERROR in result processing", error_msg)
        self.assertIn("mocked type error", error_msg)

    def test_result_processor_incremental_save(self):
        stop_event = threading.Event()
        result_queue = queue.Queue()
        progress_queue = queue.PriorityQueue()
        collisions = MagicMock()
        manifest = MagicMock()
        manifest.md5_data = {}  # Mock the underlying data store

        processor = ResultProcessor(
            stop_event,
            result_queue,
            collisions,
            manifest,
            progress_queue=progress_queue,
        )

        result_queue.put(("md5_1", 100, 123.45, "a/file/1"))
        result_queue.put(("md5_2", 200, 123.46, "a/file/2"))
        result_queue.put(("md5_3", 300, 123.47, "a/file/3"))

        processor.start()
        result_queue.join()
        stop_event.set()
        processor.join(timeout=2)

        # save should be called once after processing the third item
        manifest.save.assert_called_once()
        self.assertFalse(progress_queue.empty())

    def test_result_processor_incremental_save_error(self):
        stop_event = threading.Event()
        result_queue = queue.Queue()
        progress_queue = queue.PriorityQueue()
        collisions = MagicMock()
        manifest = MagicMock()
        manifest.md5_data = {}
        manifest.save.side_effect = OSError("mocked save error")

        processor = ResultProcessor(
            stop_event,
            result_queue,
            collisions,
            manifest,
            progress_queue=progress_queue,
        )

        result_queue.put(("md5_1", 100, 123.45, "a/file/1"))
        result_queue.put(("md5_2", 200, 123.46, "a/file/2"))
        result_queue.put(("md5_3", 300, 123.47, "a/file/3"))

        processor.start()
        result_queue.join()
        stop_event.set()
        processor.join(timeout=2)

        manifest.save.assert_called_once()
        self.assertFalse(progress_queue.empty())
        # The first message is the "Hit incremental save size..." message
        progress_queue.get()
        # The second message is the error from the save
        _priority, msg_type, path, error_msg = progress_queue.get()
        self.assertEqual(msg_type, "error")
        self.assertIn("ERROR Saving incremental", error_msg)
        self.assertIn("mocked save error", error_msg)


class TestReadThread(unittest.TestCase):
    @patch("dedupe_copy.threads.read_file", side_effect=OSError("mocked os error"))
    def test_read_thread_handles_os_error(self, mock_read_file):
        stop_event = threading.Event()
        work_queue = queue.Queue()
        result_queue = queue.Queue()
        progress_queue = queue.PriorityQueue()
        walk_config = WalkConfig()

        work_queue.put("a/file/path")

        reader = ReadThread(
            work_queue,
            result_queue,
            stop_event,
            walk_config=walk_config,
            progress_queue=progress_queue,
        )
        reader.start()

        work_queue.join()
        stop_event.set()
        reader.join(timeout=2)

        self.assertFalse(reader.is_alive(), "Reader thread should have terminated")
        self.assertTrue(result_queue.empty())
        self.assertFalse(progress_queue.empty())

        _priority, msg_type, path, error = progress_queue.get()
        self.assertEqual(msg_type, "error")
        self.assertEqual(path, "a/file/path")
        self.assertIsInstance(error, OSError)
        self.assertIn("mocked os error", str(error))

    @patch("dedupe_copy.threads.time.sleep")
    @patch("dedupe_copy.threads.read_file")
    def test_read_thread_pauses_on_save_event(
        self, mock_read_file, mock_sleep
    ):
        stop_event = threading.Event()
        save_event = threading.Event()
        work_queue = queue.Queue()
        result_queue = queue.Queue()
        walk_config = WalkConfig()
        sleep_called_event = threading.Event()

        def sleep_side_effect(duration):
            if duration == 1:
                sleep_called_event.set()

        mock_sleep.side_effect = sleep_side_effect
        work_queue.put("a/file/path")

        reader = ReadThread(
            work_queue,
            result_queue,
            stop_event,
            walk_config=walk_config,
            save_event=save_event,
        )

        save_event.set()
        reader.start()

        called = sleep_called_event.wait(timeout=2)
        self.assertTrue(called, "Thread did not call sleep(1)")

        self.assertFalse(mock_read_file.called)

        save_event.clear()
        work_queue.join()
        stop_event.set()
        reader.join(timeout=2)

        mock_read_file.assert_called_once_with("a/file/path", hash_algo="md5")
        self.assertFalse(reader.is_alive())


class TestDeleteThread(unittest.TestCase):
    def test_delete_thread_dry_run(self):
        stop_event = threading.Event()
        work_queue = queue.Queue()
        progress_queue = queue.PriorityQueue()

        work_queue.put("a/file/path")

        deleter = DeleteThread(
            work_queue,
            stop_event,
            progress_queue=progress_queue,
            dry_run=True,
        )
        deleter.start()

        work_queue.join()
        stop_event.set()
        deleter.join(timeout=2)

        self.assertFalse(deleter.is_alive())
        self.assertFalse(progress_queue.empty())

        _priority, msg_type, message = progress_queue.get()
        self.assertEqual(msg_type, "message")
        self.assertIn("[DRY RUN] Would delete a/file/path", message)


class TestWalkThread(unittest.TestCase):
    @patch("dedupe_copy.threads.time.sleep")
    @patch("dedupe_copy.threads.distribute_work")
    def test_walk_thread_pauses_on_save_event(
        self, mock_distribute_work, mock_sleep
    ):
        stop_event = threading.Event()
        save_event = threading.Event()
        walk_queue = queue.Queue()
        work_queue = queue.Queue()
        walk_config = WalkConfig()
        already_processed = set()
        sleep_called_event = threading.Event()

        def sleep_side_effect(duration):
            if duration == 1:
                sleep_called_event.set()

        mock_sleep.side_effect = sleep_side_effect
        walk_queue.put("a/dir/path")

        walker = WalkThread(
            walk_queue,
            stop_event,
            walk_config=walk_config,
            work_queue=work_queue,
            already_processed=already_processed,
            save_event=save_event,
        )

        save_event.set()
        walker.start()

        called = sleep_called_event.wait(timeout=2)
        self.assertTrue(called, "Thread did not call sleep(1)")

        self.assertFalse(mock_distribute_work.called)

        save_event.clear()
        walk_queue.join()
        stop_event.set()
        walker.join(timeout=2)

        self.assertFalse(walker.is_alive())


class TestProgressThread(unittest.TestCase):
    @patch("dedupe_copy.threads.time.sleep")
    def test_progress_thread_pauses_on_save_event(self, mock_sleep):
        stop_event = threading.Event()
        save_event = threading.Event()
        work_queue = queue.Queue()
        result_queue = queue.Queue()
        progress_queue = queue.PriorityQueue()
        walk_queue = queue.Queue()
        sleep_called_event = threading.Event()

        def sleep_side_effect(duration):
            if duration == 1:
                sleep_called_event.set()

        mock_sleep.side_effect = sleep_side_effect

        progresser = ProgressThread(
            work_queue,
            result_queue,
            progress_queue,
            walk_queue=walk_queue,
            stop_event=stop_event,
            save_event=save_event,
        )

        save_event.set()
        progresser.start()

        called = sleep_called_event.wait(timeout=2)
        self.assertTrue(called, "Thread did not call sleep(1)")

        save_event.clear()
        stop_event.set()
        progresser.join(timeout=2)

        self.assertFalse(progresser.is_alive())
