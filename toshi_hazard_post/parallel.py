import logging
import multiprocessing
import queue
import threading
import traceback
from functools import partial
from typing import TYPE_CHECKING, Callable, Tuple, Union

if TYPE_CHECKING:
    from toshi_hazard_post.aggregation_calc import AggSharedArgs

log = logging.getLogger(__name__)


def setup_parallel(
    num_workers: int,
    func: Callable,
    shared_args: 'AggSharedArgs',
) -> Tuple[Union[queue.Queue, multiprocessing.JoinableQueue], Union[queue.Queue, multiprocessing.Queue]]:
    """
    Create the task and results queus and setup workers for either parallel (multiprocessing) or serial mode

    Parameters:
        num_workers: the number of workers to create. If <1, then a serial process will be used
        func: the callable object to be invoked
        shared_args: the arguments of func common to every task

    Returns:
        task_queue: the task queue
        results_queue: the results queue
    """

    if num_workers > 1:
        return setup_multiproc(num_workers, func, shared_args)
    return setup_serial(func, shared_args)


def setup_multiproc(
    num_workers: int, func: Callable, shared_args: 'AggSharedArgs'
) -> Tuple[multiprocessing.JoinableQueue, multiprocessing.Queue]:
    log.info("creating %d multiprocessing workers" % num_workers)  # pragma: no mutate
    task_queue: multiprocessing.JoinableQueue = multiprocessing.JoinableQueue()
    result_queue: multiprocessing.Queue = multiprocessing.Queue()
    pfunc = partial(func, shared_args=shared_args)
    workers = [AggregationWorkerMP(task_queue, result_queue, pfunc) for i in range(num_workers)]
    for w in workers:
        w.start()
    return task_queue, result_queue


def setup_serial(func: Callable, shared_args: 'AggSharedArgs') -> Tuple[queue.Queue, queue.Queue]:
    log.info("creating one serial worker")  # pragma: no mutate
    task_queue: queue.Queue = queue.Queue()
    result_queue: queue.Queue = queue.Queue()
    pfunc = partial(func, shared_args=shared_args)
    worker = AggregationWorkerSerial(task_queue, result_queue, pfunc)
    worker.start()
    return task_queue, result_queue


class Worker:
    def run(self):
        log.info("worker %s running." % self.name)  # pragma: no mutate
        proc_name = self.name

        while True:
            log.debug("worker %s: requesting task" % self.name)  # pragma: no mutate
            task_args = self.task_queue.get()
            if task_args is None:
                # Poison pill means shutdown
                self.task_queue.task_done()
                log.info('%s: Exiting' % proc_name)  # pragma: no mutate
                break
            log.info("worker %s: working on hazard for site: %s, imt: %s" % (self.name, task_args.site, task_args.imt))  # pragma: no mutate

            try:
                self.func(task_args=task_args, worker_name=self.name)  # calc_aggregation
                self.task_queue.task_done()
                log.info('%s task done.' % self.name)  # pragma: no mutate
                self.result_queue.put(str(task_args.imt))
            except Exception:
                log.error(traceback.format_exc())
                args = f"{task_args.site}, {task_args.imt}"
                self.result_queue.put(f'FAILED {args} {traceback.format_exc()}')
                self.task_queue.task_done()


class AggregationWorkerSerial(Worker, threading.Thread):
    """A serial worker"""

    def __init__(self, task_queue: queue.Queue, result_queue: queue.Queue, pfunc: Callable):
        threading.Thread.__init__(self)
        self.task_queue = task_queue
        self.result_queue = result_queue
        self.func = pfunc

    def run(self):
        super().run()


class AggregationWorkerMP(Worker, multiprocessing.Process):
    """A worker that batches aggregation processing."""

    def __init__(self, task_queue: multiprocessing.JoinableQueue, result_queue: multiprocessing.Queue, pfunc: Callable):
        multiprocessing.Process.__init__(self)
        self.task_queue = task_queue
        self.result_queue = result_queue
        self.func = pfunc

    def run(self):
        super().run()
