from abc import ABCMeta
from os.path import dirname as opd, join as opj

from classad.classad import ClassAd, ExprTree
from pytest import fixture
import numpy as np
import pyghmi
import pytest

from grue.states import StateOff, StateOn
from grue.base import (
    BaseDecisionEngine, IPMIHandler, Machine, Slot, BasePool, MachineContext,
    MachineState)


@fixture
def manifest_file(request):
    return opj(opd(request.module.__file__), 'data/manifest.json')


def test_base_decision_engine():
    assert isinstance(BaseDecisionEngine, ABCMeta)
    BaseDecisionEngine.__abstractmethods__ = set()
    engine = BaseDecisionEngine()
    assert engine.eval_turn_on() is None
    assert engine.eval_turn_off() is None
    assert engine.decide() is None

    engine.jobs = [ClassAd({'foo': 'bar'})]

    assert engine.jobs == [ClassAd({'foo': 'bar'})]


def test_ipmi_handler(monkeypatch):
    def mock_init(self, *args, **kwargs):
        pass

    def mock_set_power(*args, **kwargs):
        pass

    def mock_get_power(*args, **kwargs):
        return {'powerstate': 'off'}

    monkeypatch.setattr(pyghmi.ipmi.command.Command, '__init__', mock_init)
    monkeypatch.setattr(
        pyghmi.ipmi.command.Command, 'set_power', mock_set_power)
    monkeypatch.setattr(
        pyghmi.ipmi.command.Command, 'get_power', mock_get_power)

    ipmi = IPMIHandler(
        user='user', password='password', hostname='foo.bar.baz')

    # Command should not be set until open_session is called
    assert not isinstance(ipmi.command, pyghmi.ipmi.command.Command)
    ipmi.open_session()
    assert isinstance(ipmi.command, pyghmi.ipmi.command.Command)

    # IPMI user and password must be set
    assert ipmi.user == 'user'
    assert ipmi.password == 'password'

    # IPMIHandler adds '.oob' before first . in bmc, but not in _bmc
    assert ipmi._bmc == 'foo.bar.baz'
    assert ipmi.bmc == 'foo.oob.bar.baz'

    # Test power state property
    assert ipmi.power_state is None
    assert ipmi.power_state == ipmi._power_state

    # Ensure setting power changes power_state
    ipmi.set_power('on')
    assert ipmi.power_state == 'on'

    # Ensure getting power state changes power_state
    ipmi.get_power()
    assert ipmi.power_state == 'off'


def test_machine_init(monkeypatch):
    def mock_init(self, *args, **kwargs):
        self.user = args[0]
        self.password = args[1]
        self.bmc = args[2]

    monkeypatch.setattr(IPMIHandler, '__init__', mock_init)

    machine = Machine(name='foo.bar.baz')
    assert machine.name == 'foo.bar.baz'
    assert len(machine.slots) == 0
    assert isinstance(machine.state, StateOff)
    assert len(machine) == 0  # equal to number of slots

    # Test IPMI assignment
    machine.ipmi = ['user', 'password']
    assert isinstance(machine.ipmi, IPMIHandler)
    assert machine.ipmi.user == 'user'
    assert machine.ipmi.password == 'password'
    assert machine.ipmi.bmc == 'foo.oob.bar.baz'


def test_machine_slots():
    machine = Machine(name='foo.bar.baz')

    # Add a slot matching the machine (name -> machine)
    ad = ClassAd({'Machine': 'foo.bar.baz'})
    slot = Slot(ad=ad)
    machine.add_slot(slot)
    assert slot.parent is machine
    assert len(machine.slots) == 1
    assert len(machine) == 1

    # Add a slot not matching the machine
    ad = ClassAd({'machine': 'bar.foo.baz'})
    slot = Slot(ad=ad)
    pytest.raises(ValueError, machine.add_slot, slot)


def test_machine_agg():
    def test_func():
        pass

    machine = Machine(name='foo.bar.baz')
    slot_cpus = [10, 15, 20]

    for i in slot_cpus:
        slot = Slot(ad=ClassAd({'Machine': 'foo.bar.baz', 'TotalSlotCpus': i}))
        machine.add_slot(slot)

    assert len(machine.slots) == 3
    assert len(machine) == 3
    assert machine.agg(metric='TotalSlotCpus', func=np.sum) == 45
    assert machine.agg(metric='TotalSlotCpus', func=np.mean) == 15.0

    # Use a non-permitted function
    pytest.raises(ValueError, machine.agg, 'TotalSlotCpus', test_func)


def test_slot_init():
    ad = ClassAd({'Machine': 'foo.bar.baz', 'SlotType': 'Partitionable'})
    slot = Slot(ad=ad)

    assert len(slot.jobs) == 0
    assert not slot._temp_resources
    assert slot.classad is ad
    assert slot.is_partitionable


def test_slot_manage_resources():
    ad = ClassAd(
        {'Machine': 'foo.bar.baz', 'TotalSlotDisk': 10, 'TotalSlotMemory': 10,
         'TotalSlotCpus': 10, 'TotalSlotGpus': 10})
    slot = Slot(ad=ad)
    slot.reset_resources()
    assert slot.classad['Disk'] == 10
    assert slot.classad['Memory'] == 10
    assert slot.classad['Cpus'] == 10
    assert slot.classad['GPUs'] == 10

    slot.subtract_resource('Disk', 5)
    slot.subtract_resource('Memory', 4)
    slot.subtract_resource('Cpus', 3)
    slot.subtract_resource('GPUs', 2)
    assert slot.classad['Disk'] == 5
    assert slot.classad['Memory'] == 6
    assert slot.classad['Cpus'] == 7
    assert slot.classad['GPUs'] == 8
    slot.subtract_resource('GPUs', 6)
    assert slot.classad['GPUs'] == 2
    slot.subtract_resource('GPUs', 2)
    assert slot.classad['GPUs'] == 0

    slot.reset_resources()
    assert slot.classad['Disk'] == 10
    assert slot.classad['Memory'] == 10
    assert slot.classad['Cpus'] == 10
    assert slot.classad['GPUs'] == 10


def test_slot_assign_job():
    def dummy_job(
            mem: int, disk: int, cpus: int, gpus: int, machine: str,
            arch: str):
        return ClassAd({
            'RequestMemory': mem, 'RequestDisk': disk, 'DiskUsage': disk,
            'RequestCpus': cpus, 'RequestGpus': gpus,
            'Requirements': ExprTree(
                f'(Machine == "{machine}") && '
                f'(TARGET.Arch == "{arch}") && '
                f'(TARGET.OpSys == "LINUX") && '
                f'(TARGET.Disk >= RequestDisk) && '
                f'(TARGET.Cpus >= RequestCpus) && '
                f'(TARGET.GPUs >= RequestGpus) && '
                f'(TARGET.Memory >= RequestMemory)')})

    ad = {
        'Machine': 'foo.bar.baz', 'TotalSlotDisk': 10, 'TotalSlotMemory': 10,
        'TotalSlotCpus': 10, 'TotalSlotGpus': 10, 'OpSys': 'LINUX',
        'SlotType': 'Partitionable', 'Arch': 'ppc64le'}
    slot = Slot(ad=ClassAd(ad))

    # job with resources that fits in the slot
    job = dummy_job(2, 2, 2, 2, 'foo.bar.baz', 'ppc64le')
    assert slot.assign_job(job)
    assert slot.classad['Disk'] == 8
    assert slot.classad['Memory'] == 8
    assert slot.classad['Cpus'] == 8
    assert slot.classad['GPUs'] == 8

    # assign a second job, allowed due to slot.is_partitionable == True
    job = dummy_job(2, 2, 2, 2, 'foo.bar.baz', 'ppc64le')
    assert slot.assign_job(job)
    assert slot.classad['Disk'] == 6
    assert slot.classad['Memory'] == 6
    assert slot.classad['Cpus'] == 6
    assert slot.classad['GPUs'] == 6

    # Assign a job to a slot that is not partitionable
    ad['SlotType'] = 'Static'
    slot = Slot(ad=ClassAd(ad))
    assert slot.assign_job(job)
    assert not slot.assign_job(job)  # does not fit, since slot is full
    ad['SlotType'] = 'Partitionable'
    slot = Slot(ad=ClassAd(ad))

    # Assign a job that does not fit due to memory
    job = dummy_job(12, 2, 2, 2, 'foo.bar.baz', 'ppc64le')
    assert not slot.assign_job(job)

    # Assign a job that does not fit due to architecture
    job = dummy_job(2, 2, 2, 2, 'foo.bar.baz', 'X86_64')
    slot = Slot(ad=ClassAd(ad))
    assert not slot.assign_job(job)

    # Assign a job with RequestCpus 1
    job = dummy_job(2, 2, 1, 2, 'foo.bar.baz', 'ppc64le')
    job['Requirements'] = ExprTree('(TARGET.Disk >= RequestDisk)')
    assert slot.assign_job(job)
    ad['TotalSlotCpus'] = 0
    slot = Slot(ad=ClassAd(ad))
    assert not slot.assign_job(job)  # RequestCpus should have been added


def test_base_pool():
    assert isinstance(BasePool, ABCMeta)
    BasePool.__abstractmethods__ = set()
    pool = BasePool()
    assert len(pool) == len(pool.machines) == 0
    assert pool.build('/path/to/file', 'user', 'password') is None
    assert pool.update_machine_power_state() is None
    assert pool.update_jobs() is None
    assert pool.update() is None


def test_base_pool_load_manifest(manifest_file):
    BasePool.__abstractmethods__ = set()
    pool = BasePool()
    slots = list(pool.load_manifest(manifest_file))
    assert len(slots) == 1
    assert isinstance(slots[0], ClassAd)

    # Absolute minimum property requirements
    props = {
        'Arch': 'ppc64le', 'Machine': 'gpu2.htc.inm7.de',
        'Name': 'slot1@gpu2.htc.inm7.de', 'TotalSlotCpus': 20,
        'SlotType': 'Partitionable', 'TotalSlotDisk': 895527943.0,
        'OpSys': 'LINUX', 'TotalSlotGPUs': 4, 'TotalSlotMemory': 256000,
        'HasFileTransfer': True, 'FileSystemDomain': 'juseless.inm7.de'}
    assert all([key in slots[0].keys() for key in props.keys()])
    assert all([value == slots[0][key] for key, value in props.items()])


def test_base_pool_machines(manifest_file):
    BasePool.__abstractmethods__ = set()
    pool = BasePool()

    # Populate machines and slots from manifest file
    for ad in pool.load_manifest(manifest_file):
        slot = Slot(ad=ad)
        if slot.machine in [machine.name for machine in pool.machines]:
            pool.machine[slot.machine].add_slot(slot)
        else:
            machine = Machine(name=slot.machine)
            machine.add_slot(slot)
            pool.machines.append(machine)

    assert len(pool) == len(pool.machines) == 1
    assert all([isinstance(machine, Machine) for machine in pool.machines])
    assert len(pool.slots) == 1
    assert all([isinstance(slot, Slot) for slot in pool.slots])


def test_machine_context():
    # Methods that delegate behaviour to individual states are not tested
    MachineContext.__abstractmethods__ = set()
    context = MachineContext(state=StateOff())

    assert context.machine is None
    assert context.ipmi is None
    assert context.name is None
    assert isinstance(context.state, StateOff)
    assert context.timer is None
    context.transition_to(StateOn())
    assert isinstance(context.state, StateOn)
    assert context.state.context == context
    context.timer = 1200
    assert context.timer == 1200


def test_machine_state():
    MachineState.__abstractmethods__ = set()
    state = MachineState()

    assert state.context is None
    assert state.turn_on() is None
    assert state.turn_off() is None
    assert state.verify(condor_online=False) is None

    # Test method delegation to context
    MachineContext.__abstractmethods__ = set()
    context = MachineContext(state=StateOff())
    state.context = context

    assert state.name is context.name
    assert state.ipmi is context.ipmi
    assert state.timer is context.timer
    state.timer = 1200  # state.timer delegates to context.timer
    assert context.timer == 1200
    state.transition_to(StateOn())
    assert isinstance(context.state, StateOn)
