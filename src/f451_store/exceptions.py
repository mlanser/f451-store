"""Exceptions classes for f451 Datastore module.

This module holds the custom exceptions used across all services
in the f451 Datastore module.

Note:
    This module is inspired by the "Exceptions" module in the "Notifiers" module
    by Or Carmi: https://github.com/liiight/notifiers
"""
from typing import Any

# =========================================================
#          G L O B A L S   A N D   H E L P E R S
# =========================================================
_ERROR_UNKNOWN_: str = "Unknown error"


# =========================================================
#        M A I N   C L A S S   D E F I N I T I O N
# =========================================================
class f451StoreExceptionError(Exception):
    """Exception base class for f451 Datastore module.

    Catch this exception to catch all custom exceptions from
    the f451 Datastore module.

    Looks for ``service``, ``message`` and ``data`` in kwargs

    Requires:
        'kwargs' keyword: 'errors'

    Args:
        args:
            exception arguments
        kwargs:
            exception kwargs
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.service = kwargs.get("service")
        self.message = kwargs.get("message")
        self.data = kwargs.get("data")
        self.response = kwargs.get("response")
        super().__init__(f"{self.service} - {self.message}")

    def __repr__(self) -> str:
        return f"<f451StoreError: {self.message}>"


class InvalidAttributeError(f451StoreExceptionError):
    """Invalid attribute error.

    Raised when given value is out of bounds and/or does not meet
    requirements for a given attribute/argument.

    Args:
        errMsg:
            error message for 'validation' failure
        args:
            exception arguments
        kwargs:
            exception kwargs
    """

    def __init__(self, errMsg: str, *args: Any, **kwargs: Any) -> None:
        kwargs["message"] = f"Invalid attribute error: {errMsg}"
        super().__init__(*args, **kwargs)

    def __repr__(self) -> str:
        return f"<InvalidAttributeError: {self.message}>"


class MissingAttributeError(f451StoreExceptionError):
    """Missing attribute error.

    Raised when required data attributes are missing.

    Args:
        errMsg:
            Error message for missing (required) 'attribute'
        args:
            Exception arguments
        kwargs:
            Exception kwargs
    """

    def __init__(self, errMsg: str, *args: Any, **kwargs: Any) -> None:
        kwargs["message"] = f"Missing attribute error: {errMsg}"
        super().__init__(*args, **kwargs)

    def __repr__(self) -> str:
        return f"<MissingAttributeError: {self.message}>"


class InvalidStorageError(f451StoreExceptionError):
    """Invalid storage service.

    Storage service is either unknown or not enabled.

    Args:
        storage:
            Name of storage service
        args:
            Exception arguments
        kwargs:
            Exception kwargs
    """

    def __init__(self, storage: str, *args: Any, **kwargs: Any) -> None:
        self.storage = storage
        kwargs["message"] = f"Invalid storage: {storage}"
        super().__init__(*args, **kwargs)

    def __repr__(self) -> str:
        return f"<InvalidStorageError: {self.storage}>"


class StorageAccessError(f451StoreExceptionError):
    """Storage write error.

    Unable to write s=data to storage service.

    Args:
        storage:
            Name of storage service
        args:
            Exception arguments
        kwargs:
            Exception kwargs
    """

    def __init__(self, storage: str, *args: Any, **kwargs: Any) -> None:
        self.storage = storage
        kwargs["message"] = f"Unable to write data to storage: {storage}"
        super().__init__(*args, **kwargs)

    def __repr__(self) -> str:
        return f"<InvalidStorageError: {self.storage}>"


class StorageConnectionError(f451StoreExceptionError):
    """Connection error.

    This is raised when a service returns an error.

    Requires:
        'kwargs' keyword: 'errors'

    Args:
        args:
            exception arguments
        kwargs:
            exception kwargs
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.errors = kwargs.pop("errors", None)
        kwargs["message"] = (
            _ERROR_UNKNOWN_
            if self.errors is None
            else f"Storage connection errors: {','.join(self.errors)}"
        )
        super().__init__(*args, **kwargs)

    def __repr__(self) -> str:
        return f"<StorageConnectionError: {self.message}>"
