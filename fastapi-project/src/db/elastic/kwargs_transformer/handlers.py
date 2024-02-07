from __future__ import annotations

import abc

from src.db.elastic.kwargs_transformer.constraints import SortConstraint, FilterConstraint, SearchConstraint


class BaseHandler(abc.ABC):
    """
    Interface defining base signature of each Handler.
    """

    _next_handler: BaseHandler = None

    def set_next(self, handler: BaseHandler) -> BaseHandler:
        """
        Allow chain handlers by using:
        handler1.set_next(handler2).set_next(handler3)

        :param handler: A handler which will be set next after current.
        :return: A next handler.
        """
        self._next_handler = handler
        return handler

    @abc.abstractmethod
    def handle(self, kwargs: dict):
        """
        Calls next handler to handle kwargs.
        If no handlers left, returns result kwargs.
        :param kwargs:
        :return:
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

    def handle(self, kwargs: dict):
        kwargs['body'] = self._default_body()
        return super().handle(kwargs)


class PaginationHandler(BaseHandler):
    """
    A handler for pagination.
    Handled keys: kwargs['page_number']: int, kwargs['size']: int.

    kwargs['page_number'] is a number of page to start from.
    kwargs['size'] is a count of record on every page.
    """
    def handle(self, kwargs: dict):
        if not kwargs.get('size'):
            kwargs['size'] = 100
        if kwargs.get('page_number'):
            kwargs['from_'] = (kwargs.pop('page_number') - 1) * kwargs.get('size')
        return super().handle(kwargs)


class SortHandler(BaseHandler):
    """
    A handler for sort.
    Handled key: kwargs['sort']: Optional[str].

    IMPORTANT: In chain must be set before SearchHandler.
    Otherwise, FilterHandler will write sort by its _score field as more important.

    kwargs['sort'] contains field to sort by. For ascending use: '{field}'. For descending: '-{field}'.
    """

    def handle(self, kwargs: dict):
        if kwargs.get('sort'):
            sort_constraint = SortConstraint(kwargs.pop('sort'))
            kwargs['body']['sort'].append(sort_constraint.build())
        return super().handle(kwargs)


class FilterHandler(BaseHandler):
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

    def handle(self, kwargs: dict):
        if kwargs.get('filters'):
            for q_filter in kwargs.pop('filters'):
                if None not in (q_filter['field'], q_filter['value']):
                    filter_constraint = FilterConstraint(**q_filter)
                    kwargs['body']['query']['bool'][filter_constraint.type].append(filter_constraint.build())
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
    def handle(self, kwargs: dict):
        if kwargs.get('search'):
            search_constraint = SearchConstraint(**kwargs.pop('search'))
            kwargs['body']['query']['bool']['must'].append(search_constraint.build())
            kwargs['body']['sort'].append({'_score': 'desc'})
        return super().handle(kwargs)
