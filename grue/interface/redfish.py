from typing import Tuple
import logging

from grue import base


logger = logging.getLogger(__name__)


class Redfish(base.ManagementInterface):
    def __init__(self):
        raise NotImplementedError('redfish support is not yet implemented')

    def connect_interface(self, auth: Tuple[str, str]) -> None:
        pass

    def open_session(self) -> None:
        logger.debug(f'Open redfish session to {self.bmc}')
        pass

    def close_session(self) -> None:
        logger.debug(f'Close redfish session to {self.bmc}')
        pass

    @property
    def bmc(self) -> str:
        pass

    @bmc.setter
    def bmc(self, value: str) -> None:
        pass

    @property
    def interface(self):
        pass

    @interface.setter
    def interface(self, value) -> None:
        pass

    @property
    def power(self) -> bool:
        pass

    @power.setter
    def power(self, value: int) -> None:
        pass

    @property
    def power_on(self) -> bool:
        pass

    @power_on.setter
    def power_on(self, value: bool):
        pass
