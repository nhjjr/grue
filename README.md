# grue
_It is pitch black. You are likely to be eaten by a grue._

Grue handles the power state of a subsection of machines used in an HTCondor 
pool based on user processing needs. It periodically queries a live HTCondor
pool to obtain idle jobs and determines which machines must be turned on to 
meet the requirements to process these jobs. It then evaluates which machines 
are no longer needed and shuts them down.

## Dependencies
The required dependencies to use grue are:
- `Python >= 3.7`,
- `setuptools`,
- `htcondor ~= 9.0`,
- `python-ipmi ~= 0.5`

Optionally, `rich ~= 9.10` can be installed for a better looking 
representation of `grue status`.

## Install
grue can be installed using pip by running the following terminal command:

```
pip3 install git+https://jugit.fz-juelich.de/inm7/infrastructure/loony_tools/grue.git
```

# Usage
grue comes in two parts: the daemon and the client. When the daemon is running
it will periodically execute grue's decision making process that changes the
power state of machines. The client can be used to interact with the daemon 
while it is running, for example to forcibly change the state of machines 
within grue's environment.

## Daemon
```
usage: grue-daemon [-h] [-v] {start,stop} ...

positional arguments:
  {start,stop}
```

### Starting grue
```
usage: grue-daemon start [-h] [-u USER] [-p PASSWORD] -m MANIFEST [-s STATE_FILE]

optional arguments:
  -u USER, --user USER  Management interface session username
  -p PASSWORD, --password PASSWORD
                        Management interface session password
  -m MANIFEST, --manifest MANIFEST
                        HTCondor Slots-ClassAd manifest file
  -l LOG_LEVEL, --log LOG_LEVEL
                        Log level: {debug, info, warning, error}
  -s STATE_FILE, --state-file STATE_FILE
                        File location where machine states are stored

```
Have grue build its own pool of machines from a manifest file 
(`-m`/`--manifest`) and continuously cycle through its decision-making 
process. Grue will not manage any machines that are not included in its 
manifest file. 

The login credentials are used for an IPMI connection. If given via the 
`grue-daemon start` command, only one set of login credentials will be used 
for all machines listed in the manifest. Alternatively, login credentials may 
be provided in the manifest file. The password may also be made available 
through the `IPMIPASSWORD` environmental variable, which will be unset after
use.

Optionally a `--log` level can be set to change the output grue provides. By
default grue will output INFO level log messages, but this can be set to 
DEBUG to get more detailed information.

Optionally a `--state-file` can be defined, where grue stores the state 
information it has on each machine in its pool. If no state file is given,
it by default stores this information in `/var/tmp/grue_state.json`.

Grue posts a lot of log messages while the logging level is set to debug. It 
is recommended to append ` >| grue.log 2>&1` to this command so that debug 
messages don't overflow in the terminal.

### Stopping grue
```
usage: grue-daemon stop [-h]
```

Issuing this command shuts down the grue daemon gracefully. If grue is 
currently in its decision-making process, it will finish the procedure and 
shut down afterwards.

## Client
```
usage: grue [-h] [-v] {create_manifest,state,status} ...

positional arguments:
  {create_manifest,state,status}
```

### Creating a Manifest
```
usage: grue create_manifest [-h] -f FILE

optional arguments:
  -f FILE, --file FILE  location of the output manifest file
```

Create the output file `-f`/`--file` containing the ClassAds of each currently 
available slot from the HTCondor pool using `htcondor.AdTypes.Startd`. It is 
recommended to manually edit this file to meet your expectations. 

Login credentials for the management interface (e.g., IPMI) can be added 
manually under the `ManagementInterfaces` key, for example:

```
{
    "ManagementInterfaces": {
        "gpu3.htc.inm7.de": {
            "interface": "IPMI",
            "user": "admin",
            "password": "admin"
        },
        "gpu4.htc.inm7.de": {
        ...
```

Each machine address (e.g., `gpu3.htc.inm7.de`) can have a different 
interface and login credentials. Currently only the `IPMI` interface is 
supported, but `redfish` is on the to-do list.

### Changing a Machine State
```
usage: grue state [-h] state machines [machines ...]

positional arguments:
  state       Name of the state to change machines to
  machines    List of machines to change the state of
```

While the grue daemon is running this command can forcibly change the state of 
one or more machines within grue's environment. For example, 
`grue state maintenance gpu{3..6}.htc.inm7.de` changes the state of 
`gpu3.htc...`, `gpu4`, `gpu5`, and `gpu6.htc.inm7.de` to `Maintenance`. This 
will have grue ignore these machines in the decision making process, 
considering them as unavailable. Available states are: `unavailable`, `off`, 
`on`, `booting`, `shuttingdown`, `stuck`, and `maintenance`.

### Status of the grue environment
```
usage: grue status [-h]
```
While the grue daemon is running this will list the status of all machines in 
grue's environment. If the `rich` package is installed, the table will be 
formatted nicely.

# Customization
Grue is built to be modular. Any developer may add their own decision engines,
interface, and pool. The decision engine is the procedure that has grue reach 
a conclusion on which power state to place a machine within its environment in.
The interface sits between grue and the machine, allowing grue to change the 
machine's power state. Lastly, the pool is where grue obtains user demands to 
influence its decision making.

## Decision Engine
grue builds its own pool of machines from a manifest file. Each machine is 
composed out of one or more slots, represented using the ClassAd structure
from the HTCondor Python bindings. Once a pool has been built all machines and 
slots are retained regardless of the machine's power state. Grue assigns and 
tracks the power state of each machine in its pool using a state manager and 
validates states by talking to IPMI and HTCondor every cycle.

grue is designed to be agnostic to its own stale data and updates machine 
states every cycle, assuming that HTCondor and IPMI are always more correct 
than its own state manager. The same logic is used when evaluating and 
assigning jobs to machines: If a job is idle, grue assumes that HTCondor has 
not been able to assign it to the machines currently powered on. This may be
due to all currently powered on machines being fully occupied, or none of 
those machines are capable of executing the job (and hence meeting its
requirements).

The `SequentialDecisionEngine` is the default engine that has been designed to 
be as simplistic as possible. It orders all machines first alphabetically, 
then by its state (favoring the `Off` state). It then assigns all idle jobs 
queried from the pool to the list of machines, based on requirement expression 
matches and available resources. The ordering is done to ensure that when the 
job queue does not change (as machines take time to turn on), grue does not 
issue commands for other machines to turn on. This would result in the 
unwanted scenario of turning the entire pool on just to slot a handful of 
jobs.

## Interface
Grue needs to be able to turn machines on or off. The interface determines 
how grue can do this. Currently only the `IPMI` interface is installed, 
requiring `python-ipmi` and `ipmitools`. In the future, grue will also be able 
to use the `redfish` interface.

## Pool
The pool makes up the data grue depends on to reach its decisions. The default 
pool uses HTCondor to query for idle jobs. Grue assumes that an idle jobs 
means a lack of available resources, turning on machines to meet resource 
demands. Grue furthermore looks at how long machines have been idle according
to HTCondor, and shuts down machines that have been idle for at least an hour.
