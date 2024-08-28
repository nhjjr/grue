from abc import ABC, abstractmethod
from typing import List

from grue import base


class DecisionEngine(ABC):
    _pool: base.Pool = None

    def __init__(self, pool: base.Pool):
        self.pool = pool

    def _decide(self):
        for machine in self.eval_turn_on():
            self.pool.machine[machine].turn_on()

        for machine in self.eval_turn_off():
            self.pool.machine[machine].turn_off()

    def decide(self):
        self.pool.update()
        self._decide()
        self.pool.save()
        self.pool.cleanup()

    @abstractmethod
    def eval_turn_on(self) -> List[str]:
        """Return a list of strings with all machine names that should be
        turned on."""
        pass

    @abstractmethod
    def eval_turn_off(self) -> List[str]:
        """Return a list of strings with all machine names that should be
        turned off."""
        pass

    @property
    def pool(self) -> base.Pool:
        return self._pool

    @pool.setter
    def pool(self, value: base.Pool):
        self._pool = value
