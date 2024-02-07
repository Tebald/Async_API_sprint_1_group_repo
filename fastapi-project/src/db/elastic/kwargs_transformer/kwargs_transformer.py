import logging

from src.db.elastic.kwargs_transformer.handlers import (
    BodyHandler,
    PaginationHandler,
    SortHandler,
    FilterHandler,
    SearchHandler,
)


class KwargsTransformer:
    """
    A class to parse kwargs to dict of values, which can be processed by ElasticSearch.

    Based on 'Chain of responsibility' pattern.

    Handler and related task:
        BodyHandler: Body creation. Always first.
        PaginationHandler: Pagination.
        SortHandler: Sorting.
        FilterHandler: Filtration.
        SearchHandler: Searching. Never before SortHandler in chain.

    Example:
        search = {'field': 'search_field', 'value': 'some_value'} # 'fuzziness'='auto' as default
        filter_1 = {'field': 'filter_field_1', 'value': 'some_f_1', 'type': 'must'}
        filter_2 = {'field': 'filter_field_2', 'value': 'some_f_2', 'type': 'should'}
        filter_3 = {'field': 'filter_field_3', 'value': 'some_f_3'}  # 'type'='must' as default
        filters = [filter_1, filter_2, filter_3]

        kwargs = {'page_number': 2, 'size': 10, 'sort': '-imdb_rating', 'filters': filters, 'search': search}
        new_kwargs = kt.transform(kwargs)
        print(new_kwargs)
    """

    body_handler = BodyHandler()
    pagination_handler = PaginationHandler()
    sort_handler = SortHandler()
    filter_handler = FilterHandler()
    search_handler = SearchHandler()

    body_handler.set_next(pagination_handler).set_next(sort_handler).set_next(filter_handler).set_next(search_handler)

    def transform(self, kwargs_dc) -> dict:
        new_kwargs = self.body_handler.handle(kwargs_dc)
        logging.getLogger(__name__).debug('Kwargs transformed. New kwargs: %s', new_kwargs)
        return new_kwargs


kt = KwargsTransformer()

if __name__ == '__main__':
    search = {'field': 'search_field', 'value': 'some_value'}  # 'fuzziness'='auto' as default
    filter_1 = {'field': 'filter_field_1', 'value': 'some_f_1', 'type': 'must'}
    filter_2 = {'field': 'filter_field_2', 'value': 'some_f_2', 'type': 'should'}
    filter_3 = {'field': 'filter_field_3', 'value': 'some_f_3'}  # 'type'='must' as default
    filters = [filter_1, filter_2, filter_3]

    kwargs = {'page_number': 5, 'size': 10, 'sort': '-imdb_rating', 'filters': filters, 'search': search}
    new_kw = kt.transform(kwargs)
    from pprint import pprint
    pprint(new_kw)


def get_kwargs_transformer() -> KwargsTransformer:
    return kt
