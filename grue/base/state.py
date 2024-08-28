from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Union, Callable
import functools
import logging
import time

from grue.base import error


logger = logging.getLogger(__name__)


def verify_log(name: str, grue: str, interface: str, condor: str) -> str:
    return (
        f'Fail to verify {name} state: grue={grue}, interface={interface}, '
        f'HTCondor={condor}')


def catch_interface_error(f: Callable):
    """Catch InterfaceErrors and transition associated machine to the
    Unavailable state.

    When the decorated function throws an InterfaceError, communication to
    the BMC has failed. This should not crash grue, but instead mark the
    machine as problematic (i.e., it may be unavailable). Unavailable
    machines are verified every cycle and transitioned back to a regular
    state if communication has been re-established."""

    @functools.wraps(f)
    def catch_interface_error_wrapper(self, *args, **kwargs):
        try:
            return f(self, *args, **kwargs)
        except error.InterfaceError as e:
            logger.error(f'Communication to {self.interface.bmc} failed: {e}')
            self.transition_to(Unavailable())

    catch_interface_error_wrapper.__wrapped__ = f
    return catch_interface_error_wrapper


class State(ABC):
    """Base State class containing methods and references to Context that all
    StateX classes should have."""
    _context = None

    def __repr__(self):
        return self.__class__.__name__

    def __name__(self):
        return self.__class__.__name__

    @staticmethod
    def _is_wrapped(f: Callable):
        wrapped = getattr(f, '__wrapped__', None)
        if wrapped:
            return True if wrapped.__name__ == f else False
        else:
            return False

    def __init_subclass__(cls):
        if not cls._is_wrapped(cls.turn_on):
            cls.turn_on = catch_interface_error(cls.turn_on)

        if not cls._is_wrapped(cls.turn_off):
            cls.turn_off = catch_interface_error(cls.turn_off)

        if not cls._is_wrapped(cls.verify):
            cls.verify = catch_interface_error(cls.verify)

    @property
    def context(self):
        return self._context

    @context.setter
    def context(self, context) -> None:
        self._context = context

    @property
    def name(self):
        return self.context.name

    @property
    def interface(self):
        return self.context.interface

    @property
    def timer(self):
        return self.context.timer

    @timer.setter
    def timer(self, value: Union[int, float]) -> None:
        self.context.timer = value

    def transition_to(self, state: State):
        self.context.transition_to(state)

    @abstractmethod
    def turn_on(self) -> None:
        pass

    @abstractmethod
    def turn_off(self) -> None:
        pass

    @abstractmethod
    def verify(self, htcondor_on: bool) -> None:
        pass


class Unavailable(State):
    """Something has gone wrong with the machine"""
    def turn_on(self) -> None:
        pass

    def turn_off(self) -> None:
        pass

    def verify(self, htcondor_on: bool) -> None:
        power_on = self.interface.power
        logger.debug(
            f'Verify {repr(self)} for {self.name}: htcondor_on={htcondor_on}, '
            f'interface_on={power_on}')

        if power_on and htcondor_on:
            self.context.transition_to(On())

        elif not power_on and htcondor_on:
            self.context.transition_to(Stuck())

        elif power_on and not htcondor_on:
            # Machine state unclear (likely either Booting or ShuttingDown)
            pass

        elif not power_on and not htcondor_on:
            self.context.transition_to(Off())


class Off(State):
    def turn_on(self) -> None:
        self.interface.power = 1
        self.timer = time.time()
        self.transition_to(Booting())

    def turn_off(self) -> None:
        logger.debug(
            f'Cannot turn off {self.name} as it is currently in {repr(self)}')

    def verify(self, htcondor_on: bool) -> None:
        power_on = self.interface.power
        logger.debug(
            f'Verify {repr(self)} for {self.name}: htcondor_on={htcondor_on}, '
            f'interface_on={power_on}')

        if power_on and htcondor_on:
            self.context.transition_to(On())

        elif not power_on and htcondor_on:
            self.context.transition_to(Stuck())

        elif power_on and not htcondor_on:
            # Machine state unclear (likely either Booting or ShuttingDown)
            pass


class On(State):
    def turn_on(self) -> None:
        logger.debug(
            f'Cannot turn on {self.name} as it is currently in {repr(self)}')

    def turn_off(self) -> None:
        self.interface.power = 5
        self.timer = time.time()
        self.transition_to(ShuttingDown())

    def verify(self, htcondor_on: bool) -> None:
        power_on = self.interface.power
        logger.debug(
            f'Verify {repr(self)} for {self.name}: htcondor_on={htcondor_on}, '
            f'interface_on={power_on}')

        if not power_on and not htcondor_on:
            self.context.transition_to(Off())

        elif power_on and not htcondor_on:
            self.context.transition_to(Stuck())

        elif not power_on and htcondor_on:
            # Machine state unclear (likely either Booting or ShuttingDown)
            pass


class Booting(State):
    def turn_on(self) -> None:
        logger.debug(
            f'Cannot turn on {self.name} as it is currently in {repr(self)}')

    def turn_off(self) -> None:
        logger.debug(
            f'Cannot turn off {self.name} as it is currently in {repr(self)}')

    def verify(self, htcondor_on: bool) -> None:
        if not htcondor_on:
            seconds = int(time.time() - self.timer)
            if seconds >= 900:
                logger.debug(
                    f'Transition to On period exceeded (900s) for '
                    f'{self.name}')
                self.transition_to(Stuck())
                return
            else:
                logger.debug(
                    f'{self.name} has been transitioning to On for '
                    f'{seconds}s')
                return

        elif htcondor_on:
            self.timer = None
            self.transition_to(On())
            return


class ShuttingDown(State):
    def turn_on(self) -> None:
        logger.debug(
            f'Cannot turn on {self.name} as it is currently in {repr(self)}')

    def turn_off(self) -> None:
        logger.debug(
            f'Cannot turn off {self.name} as it is currently in {repr(self)}')

    def verify(self, htcondor_on: bool) -> None:
        power_on = self.interface.power

        if power_on:
            seconds = int(time.time() - self.timer)
            if seconds >= 900:
                logger.debug(
                    f'Transition to Off period exceeded (900s) for '
                    f'{self.name}')
                self.transition_to(Stuck())
                return
            else:
                logger.debug(
                    f'{self.name} has been transitioning to Off for '
                    f'{seconds}s')
                return

        self.timer = None
        self.transition_to(Off())


class Stuck(State):
    """Something has gone wrong with the machine"""
    def turn_on(self) -> None:
        pass

    def turn_off(self) -> None:
        pass

    def verify(self, htcondor_on: bool) -> None:
        pass


class Maintenance(State):
    """The machine should be ignored by grue when it is in Maintenance state"""
    def turn_on(self) -> None:
        logger.debug(
            f'Ignore turn on {self.name} because state is {repr(self)}')

    def turn_off(self) -> None:
        logger.debug(
            f'Ignore turn off {self.name} because state is {repr(self)}')

    def verify(self, htcondor_on: bool) -> None:
        pass
