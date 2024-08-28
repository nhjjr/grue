from typing import List
import logging
import time

import htcondor
from classad.classad import ExprTree

from grue import base
from grue.base.pool import Machine


logger = logging.getLogger(__name__)


class SequentialDecisionEngine(base.DecisionEngine):
    """This decision engine allocates jobs to machines in sequential order
    until the machine is at full capacity and then determines which machines
    to turn on."""
    def eval_turn_off(self) -> List[str]:
        machines = [
            m for m in self.pool.machines
            if isinstance(m.state, base.state.On)]

        if machines:
            # update machines that are currently claimed
            claimed_machines = self.get_claimed(machines)
            for m in claimed_machines:
                m.last_active = time.time()

            machines = [m for m in machines if m not in claimed_machines]

            if not machines:
                logger.info(
                    'No machines have to be turned off (no idle machines)')
                return []

            # determine inactivity by classad property
            idle_seconds = 3600
            machines = self.filter_by_activity(machines, idle_seconds)

            # fallback determination of inactivity by Machine.state.timer
            machines = [
                m for m in machines
                if m.last_active and m.last_active+idle_seconds < time.time()]

            if not machines:
                logger.info(
                    f'No machines have to be turned off (idle machines have '
                    f'been idle less than {idle_seconds}s)')
                return []

            return [m.name for m in machines]
        else:
            logger.info(
                'No machines have to be turned off (no machines with '
                'state=On())')
            return []

    def eval_turn_on(self) -> List[str]:
        if not self.pool.jobs:
            logger.info('No machines have to be turned on (no idle jobs)')
            return []

        machines = self.reduce_machines(self.pool.machines)
        if not machines:
            logger.info(
                'No machines have to be turned on (all machines are on)')
            return []

        machines = self.sort_machines(machines)
        used_machines = []
        jobs_failed = 0
        jobs_total = 0
        jobs_assigned = 0
        for job in self.pool.jobs:
            jobs_total += 1

            # Add CPU requirement to job if not present
            req = str(job.get('Requirements', None))
            if job.get('RequestCpus', 1) == 1 and 'RequestCpus' not in req:
                expr = ExprTree(req)
                expr = expr.and_(ExprTree('(TARGET.Cpus >= RequestCpus)'))
                job['Requirements'] = expr

            for slot in [s for m in machines for s in m.slots]:
                # Minimum requirement is 1 CPU
                if slot.classad.get('Cpus', 0) <= 0:
                    continue

                if slot.assign_job(job):
                    jobs_assigned += 1
                    used_machines.append(slot.parent.name)
                    break
            else:
                jobs_failed += 1

        logger.info(
            f'Assigned {jobs_assigned} jobs and failed to assign '
            f'{jobs_failed} jobs out of {jobs_total} total evaluated '
            f'idle jobs')

        return list(set(used_machines))

    @staticmethod
    def filter_by_activity(
            machines: List[Machine],
            idle_seconds: int = 3600) -> List[Machine]:
        """Remove machines from a list if they have not been idle for at least
        the given idle_seconds (default is 1 hour)."""
        logger.debug(
            f'Query for idle machines with Activity == "Idle" && '
            f'EnteredCurrentActivity <= now() - {idle_seconds}s')
        constraint = [f'Machine == "{m.name}"' for m in machines]
        constraint = (
            f'({" || ".join(constraint)}) && State == "Unclaimed" && '
            f'Activity == "Idle" && '
            f'EnteredCurrentActivity <= {time.time() - idle_seconds}')
        result = htcondor.Collector().query(
            htcondor.AdTypes.Startd, projection=['Machine'],
            constraint=constraint)
        inactive = list(set([slot['Machine'] for slot in result]))
        return list(set([m for m in machines if m.name in inactive]))

    @staticmethod
    def get_claimed(machines: List[Machine]) -> List[Machine]:
        """Keep machines in a list if one of its slots appears to be in
        use (i.e., it is not unclaimed or drained)"""
        logger.debug(
            'Query for active machines with State!="Unclaimed" '
            '&& State!="Drained"')
        constraint = ' || '.join([f'Machine == "{m.name}"' for m in machines])
        constraint = (
            f'({constraint}) && State != "Unclaimed" '
            f'&& State != "Drained"')
        result = htcondor.Collector().query(
            htcondor.AdTypes.Startd, projection=['Machine'],
            constraint=constraint)
        claimed = list(set([slot['Machine'] for slot in result]))
        return list(set([m for m in machines if m.name in claimed]))

    @staticmethod
    def reduce_machines(machines: List[base.Machine]) -> List[base.Machine]:
        """Reduce the list of available machines by disregarding machines
        that are not in state Off or Booting."""
        return [
            m for m in machines
            if isinstance(m.state, (base.state.Off, base.state.Booting))]

    @staticmethod
    def sort_machines(machines: List[base.Machine]) -> List[base.Machine]:
        """Sort machines by name and then sort by state, listing
        state Booting first, Off second, and the rest last"""
        machines.sort(key=lambda x: x.name, reverse=False)
        sort_order = {repr(base.state.Booting()): 0, repr(base.state.Off()): 1}
        machines.sort(key=lambda x: sort_order.get(repr(x.state), 2))
        return machines
