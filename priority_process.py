import queue
import heapq
from concurrent.futures import ProcessPoolExecutor, Future
from concurrent.futures.process import (
    _WorkItem,
    _base,
    _global_shutdown,
    BrokenProcessPool,
)

"""
Modified version of the ProcessPoolExecutor that allows for priority scheduling of tasks. When submitting tasks
through the submit() method, the priority can be specified as a keyword argument. The priority is used to order tasks
within the work queue, with lower priority numbers taking precedence. 
"""

class _PriorityWorkQueue(queue.Queue):
    """A Queue that retrieves entries in priority order (lowest first)."""

    def _init(self, maxsize):
        self.queue = []  # list of (priority, count, item) tuples
        self.count = 0  # Used to break ties in priority

    def _qsize(self):
        return len(self.queue)

    def _put(self, item):
        # item is the work_id, priority is passed separately
        raise NotImplementedError("Use put(item, priority) instead")

    def put(self, item, priority=0, block=True, timeout=None):
        """Put an item into the queue with a given priority.

        Lower priority numbers are higher priority (0 is highest).
        """
        self.not_full.acquire()
        try:
            if self.maxsize > 0 and self._qsize() >= self.maxsize:
                if not block:
                    raise queue.Full
                if timeout is None:
                    self.not_full.wait()
                else:
                    self.not_full.wait(timeout)
                    if self._qsize() >= self.maxsize:
                        raise queue.Full
            self._put_with_priority(item, priority)
            self.unfinished_tasks += 1
            self.not_empty.notify()
        finally:
            self.not_full.release()

    def put_nowait(self, item, priority=0):
        """Put an item into the queue without blocking.

        Only enqueue the item if a free slot is immediately available.
        Otherwise raise the Full exception.
        """
        return self.put(item, priority, block=False)

    def _put_with_priority(self, item, priority):
        count = self.count
        self.count += 1
        heapq.heappush(self.queue, (priority, count, item))

    def _get(self):
        # Return only the item, not the priority or count
        return heapq.heappop(self.queue)[2]


class PriorityProcessPoolExecutor(ProcessPoolExecutor):
    def __init__(
        self, max_workers=None, mp_context=None, initializer=None, initargs=()
    ):
        super().__init__(max_workers, mp_context, initializer, initargs)
        # Replace the standard queue with a priority queue
        self._work_ids = _PriorityWorkQueue()

    def submit(self, fn, /, *args, priority=10, **kwargs):
        with self._shutdown_lock:
            if self._broken:
                raise BrokenProcessPool(self._broken)
            if self._shutdown_thread:
                raise RuntimeError("cannot schedule new futures after shutdown")
            if _global_shutdown:
                raise RuntimeError(
                    "cannot schedule new futures after " "interpreter shutdown"
                )

            f = _base.Future()
            w = _WorkItem(f, fn, args, kwargs)

            self._pending_work_items[self._queue_count] = w
            # Store the priority with the work_id in our custom queue
            self._work_ids.put(self._queue_count, priority)
            self._queue_count += 1
            # Wake up queue management thread
            self._executor_manager_thread_wakeup.wakeup()

            if self._safe_to_dynamically_spawn_children:
                self._adjust_process_count()
            self._start_executor_manager_thread()
            return f
