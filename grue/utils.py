from typing import List, Union
import logging
import json

import classad
import htcondor


logger = logging.getLogger(__name__)


def argument(*name_or_flags, **kwargs):
    return list(name_or_flags), kwargs


def subcommand(parent, args: List = None):
    if not args:
        args = []

    def decorator(func):
        parser = parent.add_parser(func.__name__, description=func.__doc__)
        for arg in args:
            parser.add_argument(*arg[0], **arg[1])
        parser.set_defaults(func=func)
    return decorator


class ProgramKilled(Exception):
    pass


def signal_handler(signum, frame):
    raise ProgramKilled


class ClassAdCollector:
    _collector: htcondor.Collector = htcondor.Collector()
    _ad_type: htcondor.AdTypes
    _constraint: str = 'SlotType != "Dynamic"'
    _projection: List[str] = None
    _classads: List[classad.classad.ClassAd] = None

    def __init__(
            self, ad_type: htcondor.AdTypes = htcondor.AdTypes.Startd) -> None:
        """Collect ClassAds from an ad_type with a projection and constraint
        and store them in a pickle or json file."""
        self._ad_type = ad_type
        self._classads = []
        self._projection = []

    @property
    def projection(self) -> List[str]:
        """A list of attributes to use for the projection.

        Only these attributes, plus a few server-managed, are returned in
        each ClassAd."""
        return self._projection

    @projection.setter
    def projection(self, value) -> None:
        self._projection = value

    @property
    def constraint(self) -> Union[str, classad.ExprTree]:
        """A constraint for the collector query; only ads matching this
        constraint are returned.

        If not specified, all matching ads of the given type are returned."""
        return self._constraint

    @constraint.setter
    def constraint(self, value) -> None:
        self._constraint = value

    def constraint_or(self, value) -> None:
        if isinstance(self.constraint, str):
            self.constraint += f' || {value}'
        else:
            raise ValueError('Only a str can be appended to constraints')

    def constraint_and(self, value) -> None:
        if isinstance(self.constraint, str):
            self.constraint += f' && {value}'
        else:
            raise ValueError('Only a str can be appended to constraints')

    def fetch(
            self, constraint: str = None,
            projection: List[str] = None) -> None:
        """Fetch a query built with the given AdType, constraints, and
        projections using the HTCondor collector."""
        if constraint is not None:
            self.constraint = constraint
        if projection is not None:
            self.projection = projection

        self._classads = self._collector.query(
            self._ad_type,
            constraint=self.constraint,
            projection=self.projection)

    @property
    def json_classads(self) -> dict:
        """Transform a list of classads to a json-compatible format"""
        return {
            repr(self._ad_type):
                [dict(classad) for classad in self._classads]}

    def save(self, file_path: str) -> None:
        """Store fetched ClassAds to disk in the defined file type.

        Allowed file types are 'pickle' and 'json'."""
        if not self._classads:
            raise ValueError(
                'ClassAds have not been fetched. Use the fetch() method '
                'first.')

        with open(file_path, 'w') as out_file:
            json.dump(self.json_classads, out_file, indent=4)
