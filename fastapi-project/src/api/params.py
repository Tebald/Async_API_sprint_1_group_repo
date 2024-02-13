from http import HTTPStatus

from fastapi import HTTPException


class Params:
    page: int
    size: int

    def __init__(self, page, size):
        try:
            self.page = page
            self.size = size
        except LookupError:
            raise RuntimeError("Page and Size not found.")
        self._check_params()

    def _check_params(self):
        if self.page * self.size > 10000:
            raise HTTPException(
                status_code=HTTPStatus.BAD_REQUEST,
                detail='Amount of entries > 10k is not supported. ' 'Please try to apply filters/use search endpoint.',
            )
