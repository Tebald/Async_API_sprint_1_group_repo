from __future__ import annotations

import abc
import logging
from typing import Optional

from src.utils.kwargs_transformer.constraints import SortConstraint, FilterConstraint, SearchConstraint

logger = logging.getLogger(__name__)


class BaseHandler(abc.ABC):
    """
    Interface defining base signature of each Handler.

    _next_handler is a handler, follows after current in chain.
    """

    _next_handler: Optional[BaseHandler] = None

    def set_next(self, handler: BaseHandler) -> BaseHandler:
        """
        Connect handlers in chain.
        Making chain of handlers by using:
        handler1.set_next(handler2).set_next(handler3)
        Only connects, no more usability.

        :param handler: A handler which will be set next after current.
        :return: Handler which was set next after current.
        """
        self._next_handler = handler
        return handler

    @abc.abstractmethod
    def handle(self, kwargs: dict) -> dict:
        """
        Base method, calls next handler to handle kwargs.
        When no chained handlers left, returns result kwargs.
        Method always called by concrete handler when all his handling is done.

        :param kwargs: A dict of kwargs to handle.
        """
        if self._next_handler:
            return self._next_handler.handle(kwargs)
        return kwargs


class BodyHandler(BaseHandler):
    """
    A handler to create kwargs['body'] section.

    IMPORTANT: Must be the first handler on every chain.
    """

    @staticmethod
    def _default_body() -> dict:
        return {
            'sort': [],
            'query': {
                'bool': {
                    'must': [],
                    'should': [],
                }
            },
        }

    def handle(self, kwargs: dict) -> dict:
        kwargs['body'] = self._default_body()
        return super().handle(kwargs)


class PaginationHandler(BaseHandler):
    """
    A handler for pagination.
    Handled keys: kwargs['page_number']: int, kwargs['size']: int.

    kwargs['page_number'] is a number of page to start from.
    kwargs['size'] is a count of record on every page.
    """

    def handle(self, kwargs: dict) -> dict:
        """
        Remove 'page_number'.
        Add 'from_' containing starting record number.
        """
        if kwargs.get('page_number'):
            kwargs['from_'] = (kwargs.pop('page_number') - 1) * kwargs.get('size')
        return super().handle(kwargs)


class SortHandler(BaseHandler):
    """
    A handler for sort.
    Handled key: kwargs['sort']: Optional[str].

    IMPORTANT: In chain must be set before SearchHandler.
    Otherwise, FiltersHandler will write sort by its _score field as more important.

    kwargs['sort'] contains field to sort by. For ascending use: '{field}'. For descending: '-{field}'.
    """

    def handle(self, kwargs: dict) -> dict:
        """
        Remove 'sort'.
        Add sort dict in sort list in body.
        """
        if kwargs.get('sort'):
            sort_constraint = SortConstraint(kwargs.pop('sort'))
            kwargs['body']['sort'].append(sort_constraint.build())
        return super().handle(kwargs)


class FiltersHandler(BaseHandler):
    """
    A handler for list of filters.
    Handled key: kwargs['filters']: Optional[list[dict]].

    Each filter dict has structure:
    {
        'field': A string name of the field. Nested fields can be declared by 'genre.id'
        'value': A string value of the field
        'type': *Optional* A string type of occurrence. Allowed values: ['must', 'should']. Default: 'must'
    }
    """

    @staticmethod
    def _parse_filter_constraints(filters: list[dict]):
        """
        Parse all FilterConstraint in filters.
        Log and skip all dicts which cannot be parsed.

        :param filters: list of filter dicts.
        """
        constraints = []
        for filter_query in filters:
            if filter_query and all(key in filter_query.keys() for key in ('field', 'value')):
                constraints.append(FilterConstraint(**filter_query))
            else:
                logging.error('Error while parsing filter %s. Filter skipped.', filter_query)
        return constraints

    def handle(self, kwargs: dict) -> dict:
        """
        Remove 'filters'.
        Add filter dicts in 'must' and 'should' by FilterConstraint.type field.
        """
        if kwargs.get('filters'):
            filter_constraints = self._parse_filter_constraints(filters=kwargs.pop('filters'))
            for constraint in filter_constraints:
                kwargs['body']['query']['bool'][constraint.type].append(constraint.build())
        return super().handle(kwargs)


class SearchHandler(BaseHandler):
    """
    A handler for fuzzy search.
    Handled key: kwargs['search']: Optional[dict].

    IMPORTANT:
    In chain must be declared after SortHandler.
    Otherwise, SearchHandler will write sort by its _score field as more important.

    kwargs['search'] has structure:
    {
        'field': A string name of the field where to search
        'value': A string value to search in the field
        'fuzziness': *Optional* A fuzziness value. Allowed values: ['auto', '0', '1', '2']. Default: 'auto'
    }
    """

    @staticmethod
    def _is_search_field_valid(search_field) -> bool:
        """
        Return True if 'field' and 'value' in search_field.
        Else log error and return False.
        """
        if 'field' in search_field and 'value' in search_field:
            return True
        logger.error('Error while parsing search %s. Search skipped.', search_field)
        return False

    def handle(self, kwargs: dict) -> dict:
        """
        Remove 'search'.
        Add search constraint dict to 'must' list.
        Add sort by '_score' to apply search.
        """
        search_field = kwargs.get('search')
        if search_field and self._is_search_field_valid(search_field=kwargs.pop('search')):
            search_constraint = SearchConstraint(**search_field)
            kwargs['body']['query']['bool']['must'].append(search_constraint.build())
            kwargs['body']['sort'].append({'_score': 'desc'})

        return super().handle(kwargs)
