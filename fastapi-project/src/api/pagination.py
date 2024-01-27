from fastapi_pagination import Page
from fastapi import Query


Page = Page.with_custom_options(
    size=Query(50, ge=1, le=100, alias='page_size'),
    page=Query(1, ge=1, alias='page_number')
)
