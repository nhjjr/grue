import logging
from abc import ABC, abstractmethod
from typing import Tuple


logger = logging.getLogger(__name__)


class ManagementInterface(ABC):
    _bmc: str = None
    _interface = None
    _power_on: bool = None

    def __init__(self, auth: Tuple[str, str], hostname: str):
        self.bmc = hostname
        self.connect_interface(auth)

    @abstractmethod
    def connect_interface(self, auth: Tuple[str, str]) -> None:
        pass

    @abstractmethod
    def open_session(self):
        pass

    @abstractmethod
    def close_session(self):
        pass

    @property
    @abstractmethod
    def power(self):
        """Retrieve the power state from the BMC using the desired interface.

        Preferably only call upon this property once per cycle to prevent
        overburdening the interface. IPMI is powered by a hamster on a wheel,
        so too many requests may not be dealt with properly. Instead, store
        the output in `power_on` to determine whether the machine is on or not.
        """
        pass

    @power.setter
    @abstractmethod
    def power(self, value):
        """Send a command to the BMC using the desired interface to change the
        power state of the machine."""
        pass

    @property
    @abstractmethod
    def bmc(self) -> str:
        pass

    @bmc.setter
    @abstractmethod
    def bmc(self, value: str) -> None:
        pass

    @property
    @abstractmethod
    def interface(self):
        pass

    @interface.setter
    @abstractmethod
    def interface(self, value):
        pass

    @property
    @abstractmethod
    def power_on(self) -> bool:
        pass

    @power_on.setter
    @abstractmethod
    def power_on(self, value: bool):
        pass
