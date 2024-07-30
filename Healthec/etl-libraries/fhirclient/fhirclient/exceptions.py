# coding: utf-8

import json


class ApiValueError(ValueError):
    """A custom value error class"""

    def __init__(self, msg: str) -> None:
        super(ApiValueError, self).__init__(msg)


class ApiException(Exception):
    """A custom API exception class"""

    def __init__(
        self, status: str, reason: str = None, http_err_resp: dict = None
    ) -> None:
        self.status = status
        self.reason = reason
        self.http_err_resp = http_err_resp

    def __str__(self) -> str:
        if (
            self.http_err_resp
            and self.http_err_resp.get("errors")
            and len(self.http_err_resp.get("errors", [])) > 0
        ):
            self.reason = json.dumps(self.http_err_resp.get("errors")[0])
        return (
            f"{self.status}, Reason: {self.reason}" if self.reason else f"{self.status}"
        )
