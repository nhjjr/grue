from typing import Union, Tuple
import logging

import pyipmi
import pyipmi.interfaces

from grue import base


logger = logging.getLogger(__name__)


class IPMI(base.ManagementInterface):
    _interface: Union[pyipmi.Ipmi, None] = None

    def connect_interface(self, auth: Tuple[str, str]) -> None:
        interface = pyipmi.interfaces.create_interface(
            interface='ipmitool', interface_type='lanplus')
        self.interface = pyipmi.create_connection(interface)
        self.interface.session.set_session_type_rmcp(self.bmc)
        self.interface.session.set_auth_type_user(auth[0], auth[1])
        self.interface.target = pyipmi.Target(ipmb_address=0x20)

    def open_session(self) -> None:
        """Open a command session to a BMC

        Sessions can time out in between grue cycles, making it safer to
        open and close a session per grue cycle. Do not trust the session to
        close properly, so properly space out grue cycles.
        """
        self.interface.session.establish()

    def close_session(self) -> None:
        self.interface.session.close()

    @property
    def bmc(self) -> str:
        idx = self._bmc.index('.')
        return f'{self._bmc[:idx]}.oob{self._bmc[idx:]}'

    @bmc.setter
    def bmc(self, value: str) -> None:
        self._bmc = value

    @property
    def interface(self) -> pyipmi.Ipmi:
        return self._interface

    @interface.setter
    def interface(self, value: pyipmi.Ipmi) -> None:
        self._interface = value

    @property
    def power(self) -> Union[bool, None]:
        """Retrieve the power state of the machine.

        This function returns True if the machine is turned on, False if it is
        not turned on, and None if communication to the BMC fails.
        """
        try:
            chassis_status = self.interface.get_chassis_status()
        except Exception as e:
            raise base.InterfaceError(e)

        self.power_on = chassis_status.power_on
        return self.power_on

    @power.setter
    def power(self, value: int) -> None:
        """Change the power state of the associated machine.

        Parameters
        ----------
        value : int
            * 0 -- CONTROL_POWER_DOWN
            * 1 -- CONTROL_POWER_UP
            * 2 -- CONTROL_POWER_CYCLE
            * 3 -- CONTROL_HARD_RESET
            * 4 -- CONTROL_DIAGNOSTIC_INTERRUPT
            * 5 -- CONTROL_SOFT_SHUTDOWN
        """
        options = [
            'CONTROL_POWER_DOWN', 'CONTROL_POWER_UP', 'CONTROL_POWER_CYCLE',
            'CONTROL_HARD_RESET', 'CONTROL_DIAGNOSTIC_INTERRUPT',
            'CONTROL_SOFT_SHUTDOWN']

        if value not in (1, 5):
            state = 'unknown' if value not in range(0, 6) else options[value]
            raise ValueError(
                f'grue should only {options[1]} (1) or {options[5]} (5), not '
                f'{state} ({value}')

        logger.debug(f'Issue power {options[value]} command to {self.bmc}')

        try:
            self.interface.chassis_control(value)
        except Exception as e:
            raise base.InterfaceError(e)

        self.power_on = True if value == 1 else False

    @property
    def power_on(self) -> bool:
        return self._power_on

    @power_on.setter
    def power_on(self, value: bool):
        self._power_on = value
