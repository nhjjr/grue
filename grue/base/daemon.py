from typing import List
from xmlrpc.server import SimpleXMLRPCServer
import logging
import threading
import time

from grue.base.pool import Pool
from grue.base.state import State


logger = logging.getLogger(__name__)


class Cycle(threading.Thread):
    n_cycle: int = 0

    def __init__(
            self, interval: int, pool: Pool, monitor: threading.Event):
        self.interval = interval
        self.pool = pool
        self.monitor = monitor
        self.stopped = threading.Event()
        threading.Thread.__init__(self, target=self.run)

    def decision(self):
        """Execute the decision-making process"""
        self.monitor.clear()
        self.n_cycle += 1
        logger.info(f'Initiating cycle #{self.n_cycle}')
        self.pool.decide()
        self.monitor.set()

    def run(self):
        """Continuously cycle through the decision-making process at an
        interval"""
        self.decision()
        while not self.stopped.wait(self.interval):
            self.decision()

    def start(self):
        super().start()

    def stop(self):
        logger.info('Ending grue cycles')
        self.stopped.set()
        self.join()


class GrueDaemon(threading.Thread):
    server: SimpleXMLRPCServer = None
    cycle: Cycle = None
    running: bool = False

    def __init__(
            self, host: str, port: int, pool: Pool):
        self.address = (host, port)
        self.pool = pool
        self.monitor = threading.Event()
        self.monitor.set()
        threading.Thread.__init__(self, target=self.run, daemon=True)

    def change_state(self, new_state: str, machines: List[str]) -> List[str]:
        logger.debug(
            f'Received command: grue state {new_state} {" ".join(machines)}')
        status = []
        new_state = new_state.lower()
        states = {state.__name__: state for state in State.__subclasses__()}
        states = {k.lower(): v for k, v in states.items()}

        if new_state not in states.keys():
            status.append(f'Unknown state: {new_state}')
            logger.debug(f'Unknown state: {new_state}')
            return status

        # Wait for a cycle to finish if one is currently running
        self.monitor.wait()

        for machine in machines:
            if machine in self.pool.machine.keys():
                self.pool.machine[machine].transition_to(states[new_state]())
                status.append(
                    f'Transition {machine} to {states[new_state].__name__}')
            else:
                status.append(f'Machine {machine} does not exist')
                logger.debug(f'Machine {machine} does not exist')

        return status

    def get_status(self):
        logger.debug('Received command: grue status')
        return [
            [m.name, m.state.__name__(), str(len(m)), m.timer,
             f'{int(time.time())-m.last_active}s']
            for m in self.pool.machines]

    def reload(self, manifest_file: str):
        self.pool.reload(manifest_file)

    def run(self):
        self.running = True
        self.server = SimpleXMLRPCServer(
            self.address, allow_none=True, logRequests=False)
        logger.info(f'XMLRPC Server listening on {self.address}')
        self.server.register_function(self.change_state, "change_state")
        self.server.register_function(self.get_status, "get_status")
        self.server.register_function(self.shutdown, "shutdown")
        self.server.register_function(self.reload, "reload")
        self.server.serve_forever()

    def start(self, interval: int = 60):
        self.cycle = Cycle(interval=60, pool=self.pool, monitor=self.monitor)
        self.cycle.start()
        super().start()

    def shutdown(self):
        self.running = False

    def stop(self):
        self.cycle.stop()
        logger.info('Shutting down XMLRPC Server')
        self.server.shutdown()
        self.join()
