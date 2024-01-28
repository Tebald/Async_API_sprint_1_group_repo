from fastapi import Query
from fastapi_pagination import Page

Page = Page.with_custom_options(
    size=Query(50, ge=1, le=100, alias='page_size'),
    page=Query(1, ge=1, alias='page_number')
)
