from os.path import dirname as opd, join as opj

import classad
import htcondor
import pyghmi
import pytest

from grue.base import BasePool, IPMIHandler, Machine, Slot, MachineContext
from grue.states import StateOn, StateOff, StateTurningOn, StateTurningOff
from grue.decision.sequential import GenericPool, SequentialDecisionEngine


def dummy_job(
        mem: int, disk: int, cpus: int, gpus: int, machine: str,
        arch: str):
    return classad.classad.ClassAd({
        'RequestMemory': mem, 'RequestDisk': disk, 'DiskUsage': disk,
        'RequestCpus': cpus, 'RequestGpus': gpus,
        'Requirements': classad.classad.ExprTree(
            f'(Machine == "{machine}") && '
            f'(TARGET.Arch == "{arch}") && '
            f'(TARGET.OpSys == "LINUX") && '
            f'(TARGET.Disk >= RequestDisk) && '
            f'(TARGET.Cpus >= RequestCpus) && '
            f'(TARGET.GPUs >= RequestGpus) && '
            f'(TARGET.Memory >= RequestMemory)')})


@pytest.fixture
def manifest_file(request):
    return opj(opd(request.module.__file__), 'data/manifest.json')


def test_generic_pool_build(manifest_file, monkeypatch):
    def mock_init(self, *args, **kwargs):
        pass

    # Halt IPMI service call (to prevent a `socket.gaierror` from pyghmi)
    monkeypatch.setattr(pyghmi.ipmi.command.Command, '__init__', mock_init)
    pool = GenericPool()
    pool.build(manifest_file, ipmi_user='user', ipmi_password='password')

    # There is only 1 machine in the manifest
    assert len(pool.machines) == 1

    # Manually define hostnames for IPMI calls
    bmcs = {
        'gpu1.htc.inm7.de': 'gpu1.oob.htc.inm7.de',
        'gpu2.htc.inm7.de': 'gpu2.oob.htc.inm7.de'}

    # Test manifest loading
    for machine in pool.machines:
        assert isinstance(machine, Machine)
        assert isinstance(machine.ipmi, IPMIHandler)
        assert machine.ipmi.bmc == bmcs[machine.name]

        # There is only 1 slot in the machine
        assert len(machine.slots) == 1
        for slot in machine.slots:
            assert isinstance(slot, Slot)


def test_generic_pool_update_jobs(monkeypatch):
    def mock_init(self, *args, **kwargs):
        pass

    def mock_xquery(self, *args, **kwargs):
        return [classad.classad.ClassAd({'foo': 'bar'})]

    # HTCondor queries will not work on a system without HTCondor
    monkeypatch.setattr(htcondor.Schedd, '__init__', mock_init)
    monkeypatch.setattr(htcondor.Schedd, 'xquery', mock_xquery)
    pool = GenericPool()
    pool.update_jobs()

    # idle jobs must be added to engine.jobs
    for job in pool.jobs:
        assert list(job.keys()) == ['foo']
        assert list(job.values()) == ['bar']


def test_generic_pool_update_machine_power_state(manifest_file, monkeypatch):
    def mock_init(*args, **kwargs):
        pass

    def mock_query_correct(self, *args, **kwargs):
        return [classad.classad.ClassAd({'Machine': 'gpu2.htc.inm7.de'})]

    def mock_query_incorrect(self, *args, **kwargs):
        return [classad.classad.ClassAd({'Machine': 'foo.bar.baz'})]

    def mock_verify_true(self, *args, **kwargs):
        assert args[0]

    def mock_verify_false(self, *args, **kwargs):
        assert not args[0]

    monkeypatch.setattr(pyghmi.ipmi.command.Command, '__init__', mock_init)

    # Assess condor_on as True
    monkeypatch.setattr(htcondor.Collector, 'query', mock_query_correct)
    monkeypatch.setattr(MachineContext, 'verify_state', mock_verify_true)
    pool = GenericPool()
    pool.build(manifest_file, ipmi_user='user', ipmi_password='password')
    pool.update_machine_power_state()

    # Assess condor_on as False
    monkeypatch.setattr(htcondor.Collector, 'query', mock_query_incorrect)
    monkeypatch.setattr(MachineContext, 'verify_state', mock_verify_false)
    pool = GenericPool()
    pool.build(manifest_file, ipmi_user='user', ipmi_password='password')
    pool.update_machine_power_state()


def test_sequential_decision_engine_decide(manifest_file, monkeypatch):
    def mock_init(self, *args, **kwargs):
        pass

    def mock_set_power(self, power_state: str):
        self.power_state = power_state

    def mock_update(self, *args, **kwargs):
        pass

    def mock_eval_turn_on(self, *args, **kwargs):
        return ['gpu2.htc.inm7.de']

    def mock_eval_turn_on_empty(self, *args, **kwargs):
        return []

    def mock_eval_turn_off(self, *args, **kwargs):
        return ['gpu2.htc.inm7.de']

    def mock_eval_turn_off_empty(self, *args, **kwargs):
        return []

    monkeypatch.setattr(IPMIHandler, '__init__', mock_init)
    monkeypatch.setattr(IPMIHandler, 'set_power', mock_set_power)
    monkeypatch.setattr(BasePool, 'update', mock_update)
    monkeypatch.setattr(
        SequentialDecisionEngine, 'eval_turn_on', mock_eval_turn_on)
    monkeypatch.setattr(
        SequentialDecisionEngine, 'eval_turn_off', mock_eval_turn_off_empty)
    engine = SequentialDecisionEngine()
    engine.build(manifest_file, ipmi_user='user', ipmi_password='password')
    assert isinstance(engine.machines[0].state, StateOff)
    engine.decide()
    assert isinstance(engine.machines[0].state, StateTurningOn)

    monkeypatch.setattr(
        SequentialDecisionEngine, 'eval_turn_on', mock_eval_turn_on_empty)
    monkeypatch.setattr(
        SequentialDecisionEngine, 'eval_turn_off', mock_eval_turn_off_empty)
    engine = SequentialDecisionEngine()
    engine.build(manifest_file, ipmi_user='user', ipmi_password='password')
    assert isinstance(engine.machines[0].state, StateOff)
    engine.decide()
    assert isinstance(engine.machines[0].state, StateOff)

    monkeypatch.setattr(
        SequentialDecisionEngine, 'eval_turn_on', mock_eval_turn_on_empty)
    monkeypatch.setattr(
        SequentialDecisionEngine, 'eval_turn_off', mock_eval_turn_off)
    engine = SequentialDecisionEngine()
    engine.build(manifest_file, ipmi_user='user', ipmi_password='password')
    engine.machines[0].transition_to(StateOn())
    assert isinstance(engine.machines[0].state, StateOn)
    engine.decide()
    assert isinstance(engine.machines[0].state, StateTurningOff)


def test_sequential_decision_engine_eval_turn_on_no_jobs(
        manifest_file, monkeypatch):
    def mock_init(self, *args, **kwargs):
        pass

    monkeypatch.setattr(pyghmi.ipmi.command.Command, '__init__', mock_init)
    engine = SequentialDecisionEngine()
    engine.build(manifest_file, ipmi_user='user', ipmi_password='password')
    engine.jobs = []
    assert engine.eval_turn_on() == []


def test_sequential_decision_engine_eval_turn_on_all_machines_occupied(
        manifest_file, monkeypatch):
    """The dummy job fits in the machine slot, but the machine the slot
    belongs to is marked as StateOn, and hence is assumed to be occupied."""
    def mock_init(self, *args, **kwargs):
        pass

    monkeypatch.setattr(pyghmi.ipmi.command.Command, '__init__', mock_init)
    engine = SequentialDecisionEngine()
    engine.build(manifest_file, ipmi_user='user', ipmi_password='password')
    engine.machines[0].transition_to(StateOn())  # marks machine as occupied
    engine.jobs = [dummy_job(1, 1, 1, 1, 'gpu2.htc.inm7.de', 'ppc64le')]
    assert engine.eval_turn_on() == []


def test_sequential_decision_engine_eval_turn_on_use_machine(
        manifest_file, monkeypatch):
    def mock_init(self, *args, **kwargs):
        pass

    monkeypatch.setattr(pyghmi.ipmi.command.Command, '__init__', mock_init)
    engine = SequentialDecisionEngine()
    engine.build(manifest_file, ipmi_user='user', ipmi_password='password')
    engine.machines[0].transition_to(StateOff())  # marks machine as unoccupied
    engine.jobs = [dummy_job(1, 1, 1, 1, 'gpu2.htc.inm7.de', 'ppc64le')]
    assert engine.eval_turn_on() == ['gpu2.htc.inm7.de']


def test_sequential_decision_engine_filter_by_activity(monkeypatch):
    # TODO: Test this as an HTCondor Query
    pass


def test_sequential_decision_engine_filter_by_claimed(monkeypatch):
    # TODO: Test this as an HTCondor Query
    pass


def test_sequential_decision_engine_eval_turn_off(manifest_file, monkeypatch):
    def mock_init(self, *args, **kwargs):
        pass

    def fail_filter(*args, **kwargs):
        return []

    def pass_filter(*args, **kwargs):
        return ['gpu2.htc.inm7.de']

    monkeypatch.setattr(pyghmi.ipmi.command.Command, '__init__', mock_init)

    # No machines in StateOn
    engine = SequentialDecisionEngine()
    engine.build(manifest_file, ipmi_user='user', ipmi_password='password')

    for machine in engine.machines:
        machine.transition_to(StateOff())

    assert engine.eval_turn_off() == []

    # All machines with StateOn are occupied (i.e., claimed)
    monkeypatch.setattr(
        SequentialDecisionEngine, 'filter_by_claimed', fail_filter)

    for machine in engine.machines:
        machine.transition_to(StateOn())

    assert engine.eval_turn_off() == []

    # All machines with StateOn are unoccupied, but not idle long enough
    monkeypatch.setattr(
        SequentialDecisionEngine, 'filter_by_claimed', pass_filter)
    monkeypatch.setattr(
        SequentialDecisionEngine, 'filter_by_activity', fail_filter)
    assert engine.eval_turn_off() == []

    # gpu2.htc.inm7.de is unoccupied and has been idle long enough
    monkeypatch.setattr(
        SequentialDecisionEngine, 'filter_by_activity', pass_filter)
    assert engine.eval_turn_off() == ['gpu2.htc.inm7.de']


def test_sequential_decision_engine_reduce_machines():
    machines = ['a.b.c', 'c.a.b', 'b.c.a', 'c.b.a', 'a.c.b', 'b.a.c']
    machines = [Machine(name=machine) for machine in machines]
    assert SequentialDecisionEngine.reduce_machines(machines) == machines

    machines[0].transition_to(StateOn())  # removed
    machines[2].transition_to(StateOn())  # removed
    machines[4].transition_to(StateOn())  # removed
    machines[5].transition_to(StateTurningOn())  # should still be in there
    reduced = [machines[1], machines[3], machines[5]]
    assert SequentialDecisionEngine.reduce_machines(machines) == reduced


def test_sequential_decision_engine_sort_machines():
    names = ['a.b.c', 'c.a.b', 'b.c.a', 'c.b.a', 'a.c.b', 'b.a.c']
    machines = [Machine(name=machine) for machine in names]
    expected = [
        machines[0], machines[4], machines[5], machines[2], machines[1],
        machines[3]]
    assert SequentialDecisionEngine.sort_machines(machines) == expected

    # List StateTurningOn first, StateOff second, the rest last
    #  note: it is still ordering alphabetically by name, but state ordering
    #  gets priority.
    machines = [Machine(name=machine) for machine in names]
    machines[0].transition_to(StateOn())  # last
    machines[2].transition_to(StateTurningOn())  # first
    expected = [
        machines[2], machines[4], machines[5], machines[1], machines[3],
        machines[0]]
    assert SequentialDecisionEngine.sort_machines(machines) == expected
