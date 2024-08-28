from __future__ import annotations
from argparse import ArgumentParser
from typing import List
from xmlrpc import client
import datetime

import htcondor

from grue import __version__, XMLRPC_HOST, XMLRPC_PORT
from grue.utils import argument, subcommand, ClassAdCollector
from grue.base import State


cli = ArgumentParser(
    description='It is pitch black. You are likely to be eaten by a grue.')
cli.add_argument('-v', '--version', action='version', version=__version__)
subparsers = cli.add_subparsers(dest="subcommand")


def main():
    args = cli.parse_args()
    if args.subcommand is None:
        cli.print_help()
    else:
        args.func(args)


@subcommand(
    subparsers,
    [argument(
        "-f", "--file", type=str, dest='file', required=True,
        help="location of the output manifest file")])
def create_manifest(args):
    collector = ClassAdCollector(htcondor.AdTypes.Startd)

    # List machines and remove gpu1 and gpu2
    machines = [f'gpu{m}.htc.inm7.de' for m in list(range(1, 20))]
    machines.remove('gpu1.htc.inm7.de')
    machines.remove('gpu2.htc.inm7.de')

    collector.constraint = 'SlotType != "Dynamic"'
    for i, machine in enumerate(machines):
        if i == 0:
            collector.constraint_and(f'(Machine == "{machine}"')
        elif i == len(machines)-1:
            collector.constraint_or(f'Machine == "{machine}")')
        else:
            collector.constraint_or(f'Machine == "{machine}"')

    collector.projection = [
        'SlotID', 'Name', 'Arch', 'OpSys', 'SlotType', 'Machine',
        'TotalSlotCpus', 'TotalSlotDisk', 'TotalSlotMemory', 'TotalSlotGPUs',
        'HasFileTransfer', 'FileSystemDomain']
    collector.fetch()
    collector.save(args.file)


@subcommand(
    subparsers, [
        argument(
            type=str, dest='state',
            help=(
                f'Name of the state to change machines to. Possible states: '
                f'{[s.__name__ for s in State.__subclasses__()]}')),
        argument(
            type=str, dest='machines', nargs="+",
            help='List of machines to change the state of')])
def state(args):
    proxy = client.ServerProxy(
        f'http://{XMLRPC_HOST}:{XMLRPC_PORT}/', allow_none=True)

    try:
        results = proxy.change_state(args.state, args.machines)
    except ConnectionRefusedError:
        print('grue-daemon cannot be reached')
        return

    for result in results:
        print(result)


@subcommand(subparsers, [
    argument(
        '-H', type=bool, dest='machine_readable',
        help='remove table formatting', const=True, nargs='?', default=False,
        required=False),
    argument(
        type=str, dest='machines', nargs='*',
        help='List of machines to retrieve the status of')])
def status(args):
    def formatted_table(machines: List[List[str]]):
        console = Console()
        table = Table(show_header=True, header_style='bold cyan',
                      show_edge=True)
        table.add_column('Machine', justify='right')
        table.add_column('State', justify='left')
        table.add_column('Slots', justify='left')
        table.add_column('TransitionTimer', justify='left')
        table.add_column('Last-Active (Grue)', justify='left')
        table.add_column('Last-Active (HTCondor)', justify='left')

        for machine in machines:
            table.add_row(*machine)

        console.print(table)

    def simple_table(
            machines: List[List[str]], machine_readable: bool = False):
        headers = [
            'Machine', 'State', 'Slots', 'TransitionTimer',
            'Last-Active (Grue)', 'Last-Active (HTCondor)']
        # row_format = '{:>20}' * (len(headers))
        row_format = '{}' + '\t{}' * (len(headers) - 1)

        if not machine_readable:
            print(row_format.format(*headers))

        for row in machines:
            print(row_format.format(*row))

    def add_idle_time(machines: list):
        constraint = [f'Machine == "{m[0]}"' for m in machines]
        constraint = f'{" || ".join(constraint)}'
        result = htcondor.Collector().query(
            htcondor.AdTypes.Startd,
            projection=[
                'Machine', 'EnteredCurrentActivity', 'State', 'Activity'],
            constraint=constraint)

        # Remove machines for which at least 1 slot is active
        not_idle = list(set([
            slot['Machine'] for slot in result
            if slot['Activity'] != "Idle" or slot['State'] != "Unclaimed"]))
        result = [slot for slot in result if slot['Machine'] not in not_idle]

        # Filter to lowest idle time
        idle_time = {slot['Machine']: [] for slot in result}
        for slot in result:
            idle_time[slot['Machine']].append(int(
                datetime.datetime.now().timestamp() -
                slot['EnteredCurrentActivity']))

        idle_time = {k: min(v) for k, v in idle_time.items()}

        # Add the new data to machines
        for machine in machines:
            if machine[1] == 'On':
                machine.append(
                    f'{idle_time[machine[0]]}s'
                    if idle_time.get(machine[0], None)
                    else '0s')
            else:
                machine.append('-')

        return machines

    # Get grue machine status
    proxy = client.ServerProxy(
        f'http://{XMLRPC_HOST}:{XMLRPC_PORT}/', allow_none=True)

    try:
        machines = proxy.get_status()
    except ConnectionRefusedError:
        print('grue-daemon cannot be reached')
        return

    if args.machines:
        if isinstance(args.machines, str):
            args.machines = [args.machines]

        machines = [m for m in machines if m[0] in args.machines]

    machines = sorted(machines, key=lambda x: x[0])
    machines = add_idle_time(machines)

    # Format TransitionTimer
    now = datetime.datetime.now().timestamp()
    for m in machines:
        m[3] = f'{int(now-m[3])}s' if m[3] else '-'

    try:
        from rich.console import Console
        from rich.table import Table

        if args.machine_readable:
            simple_table(machines, True)
        else:
            formatted_table(machines)

    except ImportError:
        simple_table(machines)
