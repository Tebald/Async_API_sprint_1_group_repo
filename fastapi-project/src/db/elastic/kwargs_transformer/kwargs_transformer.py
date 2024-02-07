import logging

from src.db.elastic.kwargs_transformer.handlers import (
    BodyHandler,
    PaginationHandler,
    SortHandler,
    FiltersHandler,
    SearchHandler,
)


class KwargsTransformer:
    """
    A class to parse kwargs to dict of values, which can be processed by ElasticSearch.

    Based on 'Chain of responsibility' pattern:
    https://refactoring.guru/ru/design-patterns/chain-of-responsibility/python/example

    Handlers and related tasks:
        BodyHandler: Body creation. Always first.
        PaginationHandler: Pagination.
        SortHandler: Sorting.
        FiltersHandler: Filtration.
        SearchHandler: Searching. Never before SortHandler in chain.
    More info about fields structure and types in related handler class.

    Example:
        search = {'field': 'search_field', 'value': 'some_value'} # 'fuzziness'='auto' as default
        filter_1 = {'field': 'filter_field_1', 'value': 'some_f_1', 'type': 'must'}
        filter_2 = {'field': 'filter_field_2', 'value': 'some_f_2', 'type': 'should'}
        filter_3 = {'field': 'filter_field_3', 'value': 'some_f_3'}  # 'type'='must' as default
        filters = [filter_1, filter_2, filter_3]

        kwargs = {'page_number': 2, 'size': 10, 'sort': '-imdb_rating', 'filters': filters, 'search': search}
        new_kwargs = kt.transform(kwargs)
        from pprint import pprint
        pprint(new_kwargs)
    """

    body_handler = BodyHandler()
    pagination_handler = PaginationHandler()
    sort_handler = SortHandler()
    filter_handler = FiltersHandler()
    search_handler = SearchHandler()

    body_handler.set_next(pagination_handler).set_next(sort_handler).set_next(filter_handler).set_next(search_handler)

    def transform(self, kwargs: dict | None) -> dict:
        """
        Call self.body_handler to start chain of handlers to transform kwargs.

        Available kwargs (but not required):
            'size': int. Used to set count of returned records.
            'page_number': int. Used with 'size' by PaginationHandler to build pagination kwargs.
            'sort': string. Used by SortHandler to fill 'sort' list of constraints.
            'filters': list of dicts like {'field': str, 'value': str, 'type': Optional[str]}.
                Used by FiltersHandler to fill 'must' and 'should' lists of constraints.
            'search': dict like {'field': str, 'value': str, 'fuzziness': Optional[str]}.
                Used by SearchHandler to append 'must' list by search constraint
                and to append 'sort' list by {'_score': 'desc'} constraint.
        More information about a specific parameter you can find in the related class.

        Any other params will be left without changes.
        :return: A dict of kwargs, processed by handlers and ready to unpack in ES.search().
        """
        new_kwargs = self.body_handler.handle(kwargs)
        logging.getLogger(__name__).debug('Kwargs transformed. New kwargs: %s', new_kwargs)
        return new_kwargs


kt = KwargsTransformer()


def get_kwargs_transformer() -> KwargsTransformer:
    return kt
