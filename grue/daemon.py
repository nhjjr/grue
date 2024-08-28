from __future__ import annotations
from xmlrpc import client
from argparse import ArgumentParser
import logging
import os
import signal
import sys
import time

from grue import __version__, XMLRPC_HOST, XMLRPC_PORT
from grue.base import GrueDaemon
from grue.pool import HTCondorPool
from grue.utils import signal_handler, ProgramKilled, argument, subcommand


# logger = logging.getLogger(__name__)

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
    subparsers, [
        argument(
            '-u', '--user', type=str, required=False, dest='user',
            help='Management interface session username'),
        argument(
            '-p', '--password', type=str, required=False, dest='password',
            help='Management interface session password'),
        argument(
            '-m', '--manifest', type=str, required=True, dest='manifest',
            help='HTCondor Slots-ClassAd manifest file'),
        argument(
            '-l', '--log', type=str, required=False, dest='log_level',
            default='info', help='Log level: {debug, info, warning, error}'),
        argument(
            '-s', '--state-file', type=str, required=False,
            default='/var/tmp/grue_state.json', dest='state_file',
            help='File location where machine states are stored')])
def start(args):
    """Start the grue-daemon and XMLRPC Server"""

    logging.basicConfig(
        level=logging.WARNING,
        format=(
            '[%(levelname)s:%(filename)s:line %(lineno)s:%(funcName)s()] '
            '%(message)s'))
    logger = logging.getLogger('grue')
    log_level = logging.getLevelName(args.log_level.upper())
    logger.setLevel(level=log_level)

    if not args.password and 'IPMIPASSWORD' in os.environ:
        args.password = os.environ['IPMIPASSWORD']
        os.environ['IPMIPASSWORD'] = ""

    # Signal catching
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    # Set up the pool
    pool = HTCondorPool()
    pool.engine = 'SequentialDecisionEngine'
    pool.default_interface = 'IPMI'
    pool.state_file = args.state_file
    pool.interface_session_auth = (args.user, args.password)
    pool.populate(args.manifest)
    pool.load(args.state_file)

    # Start Grue Daemon
    daemon = GrueDaemon(host=XMLRPC_HOST, port=XMLRPC_PORT, pool=pool)
    daemon.start(interval=60)

    while daemon.running:
        try:
            time.sleep(0.1)
        except ProgramKilled:
            break

    daemon.stop()
    sys.exit(0)


@subcommand(subparsers, [])
def stop(args):
    """Stop the grue-daemon and XMLRPC Server"""

    proxy = client.ServerProxy(
        f'http://{XMLRPC_HOST}:{XMLRPC_PORT}/', allow_none=True)

    try:
        proxy.shutdown()
    except ConnectionRefusedError:
        print('grue-daemon cannot be reached')
        return

    print('grue-daemon has been shut down')


@subcommand(subparsers, [
    argument(
        '-m', '--manifest', type=str, required=False, dest='manifest',
        help='HTCondor Slots-ClassAd manifest file')])
def reload(args):
    """Reestablish grue's pool from a new manifest file"""

    proxy = client.ServerProxy(
        f'http://{XMLRPC_HOST}:{XMLRPC_PORT}/', allow_none=True)

    try:
        proxy.reload(args.manifest)
    except ConnectionRefusedError:
        print('grue-daemon cannot be reached')
        return

    print('grue-daemon has reloaded its manifest file')
