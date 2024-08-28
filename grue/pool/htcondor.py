from __future__ import annotations
import json
import logging
from typing import Dict, List, Union

from classad.classad import ClassAd
import htcondor

from grue import base

logger = logging.getLogger(__name__)


class Machine(base.Machine):
    _slots: List[Slot]

    def __len__(self) -> int:
        """The length of a machine is its number of slots"""
        return len(self.slots)

    def __repr__(self) -> str:
        return (f'{self.__class__.__name__}'
                f'(name={repr(self.name)}, n_slots={len(self.slots)}, '
                f'state={repr(self.state)})')

    def add_slot(self, slot: Slot):
        if slot.machine != self.name:
            raise ValueError(
                f'Slot ({slot.machine}) to Machine ({self.name}) mismatch')

        slot.parent = self
        self.slots.append(slot)

    @property
    def slots(self) -> List[Slot]:
        return self._slots

    @slots.setter
    def slots(self, value) -> None:
        self._slots = value


class Slot(object):
    _classad: ClassAd = None
    _jobs: List[ClassAd] = None
    _parent: Machine = None
    _tmp_resources: Dict = None

    def __init__(self, ad: ClassAd) -> None:
        """Object for holding slot data"""
        self._jobs = []
        self._temp_resources = {}
        self.classad = ad
        self.reset_resources()

    def __repr__(self):
        return '{0}({1})'.format(
            self.__class__.__name__,
            ', '.join([f'{k}={v}' for k, v in self.classad.items()]))

    @property
    def classad(self) -> ClassAd:
        return self._classad

    @classad.setter
    def classad(self, value: ClassAd):
        self._classad = value

    @property
    def parent(self) -> Machine:
        return self._parent

    @parent.setter
    def parent(self, value: Machine) -> None:
        self._parent = value

    @property
    def machine(self) -> str:
        return self.classad.get('Machine', None)

    @property
    def name(self) -> str:
        return self.classad.get('Name', 'unknown')

    @property
    def jobs(self) -> List[ClassAd]:
        return self._jobs

    @jobs.setter
    def jobs(self, value: List[ClassAd]) -> None:
        self._jobs = value

    @property
    def partitionable(self) -> bool:
        return True if self.classad.get('SlotType', None) == 'Partitionable' \
            else False

    def reset_resources(self):
        """Reset job allocation and set dynamic resource values to their
        respective totals.

        Dynamic values such as TotalSlotDisk, TotalSlotGpus, etc. are not
        used when evaluating the ExprTree. Instead, their dynamic counterparts
        Disk, Gpus, etc. are used. Since these values should be equivalent to
        their Total variants when the machine is unused we'll define them in
        this function.
        """
        self.jobs = []
        for param in ['Disk', 'Memory', 'Cpus', 'GPUs']:
            # .capitalize() is only in here for the capitalization
            # inconsistency with GPUs and TotalSlotGpus
            attr = self.classad.get(f'TotalSlot{param.capitalize()}', None)
            if attr:
                self.classad[param] = attr

    def subtract_resource(self, metric: str, x: Union[int, float]) -> None:
        """Subtract x from resource metric"""
        if self.classad.get(metric, None):
            self.classad[metric] -= x

    def assign_job(self, job: ClassAd) -> bool:
        """Assigns a job to this slot and returns True if it can do so, False
        if the slot has insufficient resources or the requirements expression
        fails to pass."""

        if not self.partitionable and self.jobs:
            return False

        elif not self.partitionable:
            if self.classad.matches(job):
                self.jobs.append(job)
                return True
            else:
                return False

        elif self.partitionable:
            if not self.classad.matches(job):
                return False

            self.subtract_resource('Disk', job.get('DiskUsage', 0))
            self.subtract_resource('Memory', job.get('RequestMemory', 0))
            self.subtract_resource('Cpus', job.get('RequestCpus', 0))
            self.subtract_resource('GPUs', job.get('RequestGpus', 0))
            self.jobs.append(job)
            return True


class HTCondorPool(base.Pool):
    def _cleanup(self):
        for slot in self.slots:
            slot.reset_resources()

    def _populate(self, manifest_file: str) -> None:
        """Populate the pool with Machines"""
        logger.debug(f'Populate Pool using manifest_file={manifest_file}')
        with open(manifest_file, 'r') as json_file:
            manifest = json.load(json_file)

            if not manifest.get('ManagementInterfaces', None):
                raise ValueError('Missing ManagementInterfaces in manifest')

            if not manifest.get('htcondor.htcondor.AdTypes.Startd', None):
                raise ValueError(
                    'Missing htcondor.htcondor.AdTypes.Startd in manifest')

            interface = manifest['ManagementInterfaces']
            slots = manifest['htcondor.htcondor.AdTypes.Startd']

            for slot in slots:
                slot = Slot(ad=ClassAd(slot))
                if slot.machine not in [m.name for m in self.machines]:
                    self.add_machine(
                        name=slot.machine, interface=interface[slot.machine])

                self.machine[slot.machine].add_slot(slot)

    def _add_machine(
            self, name: str, interface: base.ManagementInterface) -> None:
        machine = Machine(name=name, state=base.state.Off())
        machine.interface = interface
        logger.debug(f'Add {machine}')
        self.machines.append(machine)

    @property
    def slots(self) -> List[Slot]:
        return [s for slots in [m.slots for m in self.machines] for s in slots]

    def _update(self):
        pass

    def get_jobs(self) -> None:
        """Return job ClassAds that report being idle.

        List of ClassAd.JobStatus values:
          0  Unexpanded     U
          1  Idle           I
          2  Running        R
          3  Removed        X
          4  Completed      C
          5  Held           H
          6  Submission_err E
        """
        projection = [
            'RequestCpus', 'RequestGpus', 'RequestMemory', 'RequestDisk',
            'Requirements', 'GlobalJobId']
        constraint = 'JobStatus == 1 && MyType == "job"'
        logger.debug(f'Query HTCondor for idle jobs where {constraint})')
        schedd = htcondor.Schedd()
        query = schedd.query(projection=projection, constraint=constraint)
        self.jobs = query

    def get_machine_power_state(self) -> None:
        """Verify online status of listed machines.

        Collect all slot ClassAds with a projection of the machine they
        belong to that appears in the manifest from HTCondor. If the machine
        appears in the ClassAds it is considered online, otherwise it is
        considered offline."""
        collector = htcondor.Collector()
        names = [m.name for m in self.machines]
        expr = ' || '.join([f'Machine == "{name}"' for name in names])
        expr = f'SlotType != "Dynamic" && {expr}'
        logger.debug('Query HTCondor to assess which machines are available')
        response = collector.query(
            htcondor.AdTypes.Startd, constraint=expr, projection=['Machine'])

        # Verify Machine State
        for machine in self.machines:
            condor_on = True if machine.name in [
                ad['Machine'] for ad in set(response)] else False

            machine.verify_state(condor_on)
