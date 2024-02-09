import abc
from typing import Literal


class Constraint(abc.ABC):
    """
    A base abstract class of Constraint.

    Constraint here is an every statement, which changes data structure.
    Multiple constraints will be build one-by-one and included in the result body.
    """

    @abc.abstractmethod
    def build(self) -> dict:
        """
        Build from self.Constraint new dict which can be used by Elasticsearch
        """


class FilterConstraint(Constraint):
    """
    A constraint for filtering.
    """

    def __init__(self, field: str, value: str, type: Literal['must', 'should'] = 'must'):
        """
        :param field: A field to filter. To filter for nested field use "{field_parent.field_nested}".
        :param value: A value to filter field for.
        :param type: An optional type of the boolean query. Allowed values: ['must', 'should']. Default: 'must'.

        More about 'type': https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-bool-query.html.
        """
        self.field = field
        self.value = value
        self.type = type

    def _build_constraint(self) -> dict:
        """
        Create filter dict from fields.
        """
        return {'match': {self.field: self.value}}

    def _build_nested_constraint(self) -> dict:
        """
        Create nested filter dict from fields.
        """
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
        """
        Choose structure of constraint dict and return it.
        """
        return self._build_constraint() if '.' not in self.field else self._build_nested_constraint()


class SearchConstraint(Constraint):
    """
    A constraint for searching.
    """

    def __init__(self, field: str, value: str, fuzziness: Literal['0', '1', '2', 'auto'] = 'auto'):
        """
        :param field: A field where to search.
        :param value: A value to search in field.
        :param fuzziness: An optional 'fuzziness' for search. Allowed values: ['0', '1', '2', 'auto']. Default: 'auto'.

        More about fuzziness: https://www.elastic.co/guide/en/elasticsearch/reference/8.12/common-options.html#fuzziness
        """
        self.field = field
        self.value = value
        self.fuzziness = fuzziness

    def build(self) -> dict:
        """
        Build and return a search constraint dict.
        """
        return {'match': {self.field: {'query': self.value, 'fuzziness': self.fuzziness}}}


class SortConstraint(Constraint):
    """
    A constraint for sort.
    """

    def __init__(self, sort_query: str):
        """
        :param sort_query: A string contains field to sort by. For ascending use: '{field}'. For descending: '-{field}'.
        """
        self.order = 'desc' if sort_query.startswith('-') else 'asc'
        self.field = sort_query.lstrip('-')

    def build(self) -> dict:
        """
        Build and return a sort constraint dict.
        """
        return {self.field: self.order}
