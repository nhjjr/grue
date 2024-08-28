from __future__ import annotations
from abc import ABC, abstractmethod
from typing import List, Dict, Union, Tuple
import json
import logging
import datetime
import time

from grue import base


logger = logging.getLogger(__name__)


class Pool(ABC):
    _default_interface: base.ManagementInterface = None
    _engine: base.DecisionEngine = None
    _machines: List[Machine]
    _jobs: List
    _interface_session_auth: Tuple[str, str] = None
    _interfaces: dict = None
    _state_file: str = None

    def __init__(
            self, manifest_file: str = None, state_file: str = None,
            default_interface: Union[str, base.ManagementInterface] = None,
            interface_session_auth: Tuple[str, str] = None,
            engine: Union[str, base.DecisionEngine] = None) -> None:
        self._interfaces = {
            cls.__name__: cls
            for cls in base.ManagementInterface.__subclasses__()}
        self._machines = []
        self._jobs = []
        self.state_file = state_file

        if default_interface:
            self.default_interface = default_interface

        if interface_session_auth:
            self.interface_session_auth = interface_session_auth

        if engine:
            self.engine = engine

        if manifest_file:
            self.populate(manifest_file)

            if state_file:
                self.load(state_file)

    def __repr__(self) -> str:
        return '{0}(machines={1})'.format(
            self.__class__.__name__,
            repr([machine.name for machine in self.machines]))

    def __len__(self) -> int:
        """The length of a pool is its number of machines"""
        return len(self.machines)

    @abstractmethod
    def _add_machine(
            self, name: str, interface: base.ManagementInterface) -> None:
        """Append machines to `self.machines`"""
        pass

    @abstractmethod
    def _cleanup(self):
        """Subclass-specific cleanup procedures"""
        pass

    @abstractmethod
    def _populate(self, manifest_file: str) -> None:
        """Populate the pool with Machines"""
        pass

    @abstractmethod
    def _update(self) -> None:
        """Hook to run updates after _update is called"""
        pass

    def add_machine(self, name: str, interface: dict = None) -> None:
        if not interface:
            interface = {}

        auth = interface.get('auth', self._interface_session_auth)
        interface = interface.get('interface', self.default_interface)

        if interface not in self._interfaces.keys():
            raise ValueError(f'Unrecognized interface: {interface}')

        if not auth:
            raise ValueError(f'No session authentication set for {name}')

        interface = self._interfaces[interface](auth=auth, hostname=name)
        self._add_machine(name=name, interface=interface)

    def cleanup(self):
        """Mandatory cleanup procedures"""
        self.close_interface_sessions()
        self.jobs = []
        self._cleanup()

    def decide(self):
        """Shortcut to the selected engine's decision-making process"""
        if not self.engine:
            raise ValueError('No engine selected')

        self.engine.decide()

    @property
    def default_interface(self) -> base.ManagementInterface:
        return self._default_interface

    @default_interface.setter
    def default_interface(
            self, value: Union[str, base.ManagementInterface]) -> None:
        if isinstance(value, base.ManagementInterface):
            self._default_interface = value
        elif value in self._interfaces.keys():
            self._default_interface = self._interfaces[value]
        else:
            raise ValueError(f'Unrecognized interface: {value}')

    @property
    def engine(self) -> base.DecisionEngine:
        return self._engine

    @engine.setter
    def engine(self, engine: str):
        engines = {
            cls.__name__: cls
            for cls in base.DecisionEngine.__subclasses__()}

        if engine not in engines.keys():
            raise ValueError(f'Unknown engine: {engine}')

        self._engine = engines[engine](self)

    @abstractmethod
    def get_jobs(self):
        """Determine if a machine is currently in use"""
        pass

    @abstractmethod
    def get_machine_power_state(self):
        """Verify online status of listed machines."""
        pass

    @property
    def interface_session_auth(self) -> Tuple[str, str]:
        return self._interface_session_auth

    @interface_session_auth.setter
    def interface_session_auth(self, value: Tuple[str, str]) -> None:
        self._interface_session_auth = value

    @property
    def jobs(self) -> List:
        return self._jobs

    @jobs.setter
    def jobs(self, value: List):
        self._jobs = value

    @property
    def machine(self) -> Dict[str, Machine]:
        """Transform List[Machine] to Dict[str, Machine] so that machines can
        be called by name, e.g. `machine['cpu1.htc.inm7.de']`."""
        return {machine.name: machine for machine in self.machines}

    @property
    def machines(self) -> List[Machine]:
        return self._machines

    @machines.setter
    def machines(self, value: List[Machine]) -> None:
        self._machines = value

    @property
    def state_file(self) -> str:
        return self._state_file

    @state_file.setter
    def state_file(self, value: str) -> None:
        self._state_file = value

    def populate(self, manifest_file: str) -> None:
        """Mandatory procedures prior to populating"""
        self.machines = []
        self._populate(manifest_file)

    def open_interface_sessions(self):
        """Reset and start a new IPMI session using stored credentials"""
        for machine in self.machines:
            machine.interface.open_session()

    def close_interface_sessions(self):
        for machine in self.machines:
            machine.interface.close_session()

    def load(self, state_file: str) -> None:
        """Load the machine states from a state file if one is given"""
        try:
            with open(self.state_file, 'r') as file:
                data = json.load(file)
        except json.decoder.JSONDecodeError:
            logger.warning(f'State file could not be opened')
            return
        except FileNotFoundError:
            logger.warning(f'File {self.state_file} not found')
            return

        last_save = (
            datetime.datetime.fromtimestamp(data.get('last_save', 0)) +
            datetime.timedelta(minutes=15)).timestamp()
        now = datetime.datetime.now().timestamp()

        if last_save <= now:
            logger.info(f'State file has expired by {int(now-last_save)}s')
            return
        else:
            logger.info(f'Load machine states from {self.state_file}')
            states = {s.__name__: s for s in base.State.__subclasses__()}

            for name, info in data.get('machines', {}).items():
                if name in self.machine.keys():
                    self.machine[name].transition_to(
                        states[info.get('state', 'Off')]())
                    self.machine[name].timer = info.get('timer', None)

    def reload(self, manifest_file: str):
        """Reestablish the pool from a new manifest file"""
        logger.info('Reload grue data')
        self.machines = []
        self.populate(manifest_file)
        self.load(self.state_file)

    def save(self) -> None:
        """Store state information of each machine in the pool as a json
        file"""
        if not self.state_file:
            return

        data = {
            'machines': {}, 'last_save': datetime.datetime.now().timestamp()}

        for machine in self.machines:
            data['machines'][machine.name] = {
                'state': machine.state.__name__(),
                'timer': machine.state.timer}

        with open(self.state_file, 'w') as file:
            json.dump(data, file, indent=4, sort_keys=True)

    def update(self) -> None:
        """Update machines with HTCondor Collector data"""
        self.open_interface_sessions()
        self.get_machine_power_state()

        if all([
                isinstance(m.state, (base.state.On, base.state.Booting))
                for m in self.machines]):
            logger.debug('Skip update; all machines are On')
        else:
            self.get_jobs()

        self._update()


class Machine(ABC):
    """Client interface base class for holding and changing states."""
    _state: base.State = None
    _timer: Union[int, None] = None
    _last_active: Union[int, None] = None
    _name: str = None
    _interface: base.ManagementInterface = None

    def __init__(self, name: str, state: base.State) -> None:
        self.name = name
        self._slots = []
        self._last_active = int(time.time())
        self.transition_to(state)

    def __len__(self) -> int:
        """The length of a pool is its number of machines"""
        return len(self._slots)

    @property
    def interface(self) -> base.ManagementInterface:
        return self._interface

    @interface.setter
    def interface(self, value: base.ManagementInterface) -> None:
        self._interface = value

    @property
    def name(self) -> str:
        return self._name

    @name.setter
    def name(self, value: str) -> None:
        self._name = value

    @property
    def state(self):
        return self._state

    @property
    def timer(self):
        return self._timer

    @timer.setter
    def timer(self, value: Union[int, float, None]) -> None:
        if isinstance(value, (int, float)):
            value = int(value)

        logger.debug(f'Set {self.name} transition timer to {value}')
        self._timer = value

    @property
    def last_active(self):
        return self._last_active

    @last_active.setter
    def last_active(self, value: Union[int, float, None]) -> None:
        if isinstance(value, (int, float)):
            value = int(value)

        logger.debug(f'Set {self.name} last-active timer to {value}')
        self._last_active = value

    def transition_to(self, state: base.State):
        """Change Context State at runtime"""
        logger.debug(f'Transition {self.name} to {repr(state)}')
        self._state = state
        self._state.context = self

    """Delegate behavior to the current State object where necessary"""
    def turn_on(self):
        self._last_active = int(time.time())  # reset last-active to now
        self._state.turn_on()

    def turn_off(self):
        self._state.turn_off()

    def verify_state(self, condor_online: bool):
        self._state.verify(condor_online)
