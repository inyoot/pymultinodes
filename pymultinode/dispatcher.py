"""
pymultinode.dispatcher

The dispatcher is responsible for keeping track of the workers and submitting 
tasks to them
"""
from .protocol import ConnectionLost
import eventlet


class Dispatcher(object):
    """
    The Dispatcher object keeps track of both workers and tasks
    """
    def __init__(self):
        self._workers = eventlet.queue.LightQueue()
        self._tasks = eventlet.queue.LightQueue()
        self._thread = eventlet.spawn(self._process)
        self._cpus = {}
        self._active = {}
        self._waiting_tasks = 0

    def data(self):
        workers = []
        for worker, count in self._cpus.items():
            data = worker.data()
            data['cpus'] = count
            data['active'] = self._active[worker]
            workers.append(data)
        data = {
            'workers' : workers,
            'waiting_tasks' : self._waiting_tasks
        }
        return data

    def _process(self):
        """
        This internal function fires off a task whenever
        there is a task and a worker waiting
        """
        while True:
            # the queue will block when we are out of workers
            worker = self._workers.get()
            configuration_id, task, event = self._tasks.get()
            self._active[worker] += 1
            self._waiting_tasks -= 1
            thread = eventlet.spawn(worker.do_task, configuration_id, task)
            thread.link(self._task_finished, worker, configuration_id, task, event)
    
    def _task_finished(self, thread, worker, configuration_id, task, event):
        """
        This internal function is called when a worker indicates the task is complete
        """
        try:
            # this obtains the actual result of the thread
            result = thread.wait()
            self._active[worker] -= 1
        except ConnectionLost:
            # The worker has given up, we reschedule the task
            self._tasks.put( (configuration_id, task, event) )
            self._cpus[worker] -= 1
            if self._cpus[worker] == 0:
                del self._cpus[worker]
                del self._active[worker]
        else:
            # it worked, tell the event we did it
            event.send(result)
            # put the worker back on the queue for future use
            self._workers.put(worker)

    def add_worker(self, worker, count):
        """
        Add the given worker, indicate that it can be given count tasks at once
        """

        # We record the multiple counts as different workers
        for idx in xrange(count):
            self._workers.put(worker)
        self._cpus[worker] = count
        self._active[worker] = 0

    def do_task(self, configuration_id, task):
        """
        Request that the given task be performed
        """
        # we create an event for the task and avoid actually 
        # creating a greenthread for it
        event = eventlet.event.Event()
        self._tasks.put( (configuration_id, task, event)  )
        self._waiting_tasks += 1
        return event
