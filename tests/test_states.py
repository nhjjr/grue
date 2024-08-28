from os.path import dirname as opd, join as opj
import time

from pytest import fixture

from grue.base import BasePool, MachineState, IPMIHandler, Slot, Machine
from grue.states import (
    StateOff, StateOn, StateTurningOn, StateTurningOff, StateErroredOut)


@fixture
def manifest_file(request):
    return opj(opd(request.module.__file__), 'data/manifest.json')


@fixture
def pool(manifest_file, monkeypatch):
    def mock_init(self, *args, **kwargs):
        pass

    def mock_set_power(self, power_state: str):
        self.power_state = power_state

    def mock_get_power(self):
        return self.power_state

    monkeypatch.setattr(IPMIHandler, '__init__', mock_init)
    monkeypatch.setattr(IPMIHandler, 'set_power', mock_set_power)
    monkeypatch.setattr(IPMIHandler, 'get_power', mock_get_power)

    BasePool.__abstractmethods__ = set()
    pool = BasePool()

    # Populate machines and slots from manifest file
    for ad in pool.load_manifest(manifest_file):
        slot = Slot(ad=ad)
        if slot.machine in [machine.name for machine in pool.machines]:
            pool.machine[slot.machine].add_slot(slot)
            pool.machine.ipmi = ['user', 'password', slot.machine]
        else:
            machine = Machine(name=slot.machine)
            machine.ipmi = ['user', 'password', slot.machine]
            machine.add_slot(slot)
            pool.machines.append(machine)

    return pool


class MockState(MachineState):
    """Fake state that should never occur naturally, used to verify that no
    state transitions have taken place."""
    def turn_on(self) -> None:
        pass

    def turn_off(self) -> None:
        pass

    def verify(self, condor_online: bool) -> None:
        pass


def test_state_off_turn_off(pool):
    machine = pool.machines[0]
    state = machine.state
    assert isinstance(state, StateOff)  # default state

    state.turn_off()
    assert isinstance(machine.state, StateOff)
    assert state.timer is None
    # power_state is None (unverified) as it has not been assessed
    assert state.ipmi.power_state is None


def test_state_off_turn_on(pool):
    machine = pool.machines[0]
    state = machine.state
    assert isinstance(state, StateOff)

    state.turn_on()
    assert state.ipmi.power_state == 'on'
    assert isinstance(state.timer, int)
    assert state.timer <= time.time()
    assert isinstance(machine.state, StateTurningOn)


def test_state_off_verify(pool):
    machine = pool.machines[0]
    state = machine.state
    assert isinstance(state, StateOff)  # default state

    # machine.state should transition to StateOn
    state.ipmi.power_state = 'on'
    state.verify(condor_online=True)
    assert isinstance(machine.state, StateOn)

    # machine.state should transition to StateErroredOut
    state.ipmi.power_state = 'off'
    state.verify(condor_online=True)
    assert isinstance(machine.state, StateErroredOut)

    # machine.state should not transition
    state.ipmi.power_state = 'off'
    machine.transition_to(MockState())
    state.verify(condor_online=False)
    assert isinstance(machine.state, MockState)


def test_state_on_turn_off(pool):
    machine = pool.machines[0]
    machine.transition_to(StateOn())
    state = machine.state
    assert isinstance(state, StateOn)

    state.turn_off()
    assert isinstance(state.timer, int)
    assert state.timer <= time.time()
    assert isinstance(machine.state, StateTurningOff)


def test_state_on_turn_on(pool):
    machine = pool.machines[0]
    machine.transition_to(StateOn())
    state = machine.state
    assert isinstance(state, StateOn)

    state.turn_on()
    assert isinstance(machine.state, StateOn)
    assert state.timer is None
    assert state.ipmi.power_state is None


def test_state_on_verify(pool):
    machine = pool.machines[0]
    machine.transition_to(StateOn())
    state = machine.state
    assert isinstance(state, StateOn)

    # machine.state should transition to StateOff
    state.ipmi.power_state = 'off'
    state.verify(condor_online=False)
    assert isinstance(machine.state, StateOff)

    # machine.state should transition to StateErroredOut
    state.ipmi.power_state = 'on'
    state.verify(condor_online=False)
    assert isinstance(machine.state, StateErroredOut)

    # machine.state should not change
    machine.transition_to(MockState())
    state.ipmi.power_state = 'off'
    state.verify(condor_online=True)
    assert isinstance(machine.state, MockState)


def test_state_turning_on_turn_on(pool):
    machine = pool.machines[0]
    machine.transition_to(StateTurningOn())
    state = machine.state
    assert isinstance(state, StateTurningOn)

    state.turn_on()
    assert isinstance(machine.state, StateTurningOn)
    assert state.timer is None
    assert state.ipmi.power_state is None


def test_state_turning_on_turn_off(pool):
    machine = pool.machines[0]
    machine.transition_to(StateTurningOn())
    state = machine.state
    assert isinstance(state, StateTurningOn)

    state.turn_off()
    assert isinstance(machine.state, StateTurningOn)
    assert state.timer is None
    assert state.ipmi.power_state is None


def test_state_turning_on_verify(pool):
    machine = pool.machines[0]
    machine.transition_to(StateTurningOn())
    state = machine.state
    assert isinstance(state, StateTurningOn)

    # Not enough time has passed for a state transition
    state.timer = time.time()
    state.verify(condor_online=False)
    assert isinstance(machine.state, StateTurningOn)

    # Enough time has passed for a state transition to StateErroredOut
    state.timer = time.time() - 900
    state.verify(condor_online=False)
    assert isinstance(machine.state, StateErroredOut)

    # If condor marks the machine as online, transition to StateOn
    state.verify(condor_online=True)
    assert state.timer is None
    assert isinstance(machine.state, StateOn)


def test_state_turning_off_turn_on(pool):
    machine = pool.machines[0]
    machine.transition_to(StateTurningOff())
    state = machine.state
    assert isinstance(state, StateTurningOff)

    state.turn_on()
    assert isinstance(machine.state, StateTurningOff)
    assert state.timer is None
    assert state.ipmi.power_state is None


def test_state_turning_off_turn_off(pool):
    machine = pool.machines[0]
    machine.transition_to(StateTurningOff())
    state = machine.state
    assert isinstance(state, StateTurningOff)

    state.turn_off()
    assert isinstance(machine.state, StateTurningOff)
    assert state.timer is None
    assert state.ipmi.power_state is None


def test_state_turning_off_verify(pool):
    machine = pool.machines[0]
    machine.transition_to(StateTurningOff())
    state = machine.state
    assert isinstance(state, StateTurningOff)

    # Not enough time has passed for a state transition
    state.ipmi.power_state = 'on'
    state.timer = time.time()
    state.verify(condor_online=False)  # condor_online does nothing here
    assert isinstance(machine.state, StateTurningOff)

    # Enough time has passed for a state transition to StateErroredOut
    state.ipmi.power_state = 'on'
    state.timer = time.time() - 900
    state.verify(condor_online=False)
    assert isinstance(machine.state, StateErroredOut)

    # If IPMI says the machine is offline, transition to StateOff
    state.ipmi.power_state = 'off'
    state.verify(condor_online=False)
    assert state.timer is None
    assert isinstance(machine.state, StateOff)


# TODO: Make tests for StateErroredOut
def test_state_errored_out_turn_on(pool):
    pass


def test_state_errored_out_turn_off(pool):
    pass


def test_state_errored_out_verify(pool):
    pass
