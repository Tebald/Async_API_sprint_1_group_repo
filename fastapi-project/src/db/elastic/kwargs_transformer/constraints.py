from __future__ import annotations

import abc
from typing import Literal


class Constraint(abc.ABC):
    @abc.abstractmethod
    def build(self) -> dict:
        """Build from constraint dict which can be used by Elasticsearch"""


class FilterConstraint(Constraint):
    def __init__(self, field: str, value: str, type: str = 'must'):
        self.field = field
        self.value = value
        self.type = type

    def _build_constraint(self) -> dict:
        return {'match': {self.field: self.value}}

    def _build_nested_constraint(self) -> dict:
        field_name, _, _ = self.field.partition('.')
        return {
            'nested': {
                'path': field_name,
                'query': {
                    'term': {
                        self.field: self.value,
                    }
                },
            }
        }

    def build(self) -> dict:
        return self._build_constraint() if '.' not in self.field else self._build_nested_constraint()


class SearchConstraint(Constraint):
    def __init__(self, field: str, value: str, fuzziness: Literal['0', '1', '2', 'auto'] = 'auto'):
        self.field = field
        self.value = value
        self.fuzziness = fuzziness

    def build(self) -> dict:
        return {'match': {self.field: {'query': self.value, 'fuzziness': self.fuzziness}}}


class SortConstraint(Constraint):
    def __init__(self, sort_value):
        self.field = sort_value.lstrip('-')
        self.order = 'desc' if sort_value.startswith('-') else 'asc'

    def build(self) -> dict:
        return {self.field: self.order}
