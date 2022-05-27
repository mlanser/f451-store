"""Default (base) class for SQL-type storage f451 Datastore module.

This is a base class for SQL-type storage providers (e.g. SQLite, MySQL,
PostgreSQL, etc.), and it holds some common methods and attributes used
across most/all such providers/services.
"""
import logging
import pprint
from abc import abstractmethod
from typing import Any
from typing import Callable
from typing import Dict
from typing import List
from typing import Optional
from typing import Mapping
from typing import Union

import f451_store.constants as const
from f451_store.exceptions import InvalidAttributeError
from f451_store.providers.provider import Provider

# =========================================================
#          G L O B A L S   A N D   H E L P E R S
# =========================================================
log = logging.getLogger()
pp = pprint.PrettyPrinter(indent=4)

typeDefFormats = Union[Mapping[str, Callable[[Any], Any]], Dict[str, str]]


# =========================================================
#        M A I N   C L A S S   D E F I N I T I O N
# =========================================================
class BaseSQL(Provider):
    """Base class for SQL-type data storage.

    Attributes:
        serviceType:
            data storage type (e.g. CSV, JSON, SQL, etc.)
        serviceName:
            data storage name (e.g. SQLite, MySQL, PostgreSQL, etc.)
        configSection:
            name of section in config files (e.g. f451_csv, f451_sqlite, etc.)
        dataFields:
            data field map
        dataFormats:
            data format map
    """

    def __init__(
        self,
        serviceType: str,
        serviceName: str,
        configSection: str,
        dataFields: Dict[str, str],
        dataFormats: typeDefFormats,
        **kwargs: Any,
    ) -> None:
        super().__init__(
            serviceType,
            serviceName,
            configSection,
            verify_not_empty(kwargs.get(const.KWD_DB_HOST, ''), True, const.KWD_DB_HOST),
            kwargs.get(const.KWD_DB_PORT, ''),
            kwargs.get(const.KWD_DB_NAME, ''),
            verify_not_empty(kwargs.get(const.KWD_DB_TABLE, ''), True, const.KWD_DB_TABLE),
            kwargs.get(const.KWD_DB_USER_NAME, ''),
            kwargs.get(const.KWD_DB_USER_PSWD, ''),
        )
        self._dataFields = verify_db_fields(dataFields, dataFormats, serviceName)
        self._dataFormats: typeDefFormats = dataFormats
        self._create: bool = kwargs.get(const.KWD_CREATE, False)
        self._append: bool = kwargs.get(const.KWD_APPEND, True)

    @property
    def isCreateMode(self) -> bool:
        """Return 'createMode' property."""
        return self._create

    @property
    def isAppendMode(self) -> bool:
        """Return 'appendMode' property."""
        return self._append

    @property
    def dataFields(self) -> Dict[str, str]:
        """Return 'dataFields' property."""
        return self._dataFields

    @property
    def formatMap(self) -> Dict[str, str]:
        """Return 'formatMap' property."""
        return self._dataFormats

    @property
    def appendMode(self) -> bool:
        """Return 'append' mode flag/property."""
        return self._append

    @abstractmethod
    def store_records(
        self, inData: Union[List[Dict[str, Any]], Dict[str, Any]]
    ) -> None:
        """Stub for 'store_data()' method."""
        pass

    @abstractmethod
    def retrieve_records(self, numRecs: int = 1, **kwargs: Any) -> List[Dict[str, Any]]:
        """Stub for 'retrieve_data()' method."""
        pass

    @abstractmethod
    def trim_records(self, numRecs: int = 0, **kwargs: Any) -> int:
        """Stub for 'trim_records()' method."""
        pass

    @abstractmethod
    def count_records(self, **kwargs: Any) -> int:
        """Stub for 'count_records()' method."""
        pass

    @abstractmethod
    def has_table(self, dbTable: str) -> bool:
        """Stub for 'has_table()' method."""
        pass


# =========================================================
#              U T I L I T Y   F U N C T I O N S
# =========================================================
def create_orderby_param(inStr: str, flip: bool = False) -> str:
    """Helper: flip 'orderBy' parameter.

    Args:
        inStr:
            Original 'orderBy' parameter string.
        flip:
            If 'True' then turn 'ASC' to 'DESC" and vice versa
    """

    def _flip_orderby(s: str, flg: bool = False) -> str:
        if s == 'ASC':
            return 'ASC' if not flg else 'DESC'

        return 'DESC' if not flg else 'ASC'

    parts = inStr.split('|')
    if len(parts) < 1:
        return ''

    outStr = 'ASC' if len(parts) == 1 else parts[1].upper()
    return f"ORDER BY {parts[0]} {_flip_orderby(outStr, flip)}"


def verify_not_empty(inStr: str, strict: bool, varName: Optional[str] = None) -> str:
    """Verify that a given string is not empty.

    Args:
        inStr:
            String to check.
        strict:
            If 'True' then exception is raised when string is empty
        varName:
            Name of string variable to check.

    Returns:
        Given string if not empty and 'strict' is 'True'. This will
        also return an empty string if the string is empty and
        'strict' is 'False'.

    Raises:
        InvalidAttributeError: If string is empty
    """
    tmpStr = inStr.strip()

    if strict and not tmpStr:
        log.error(f"{'<blank>' if not varName else varName} string is empty.")
        raise InvalidAttributeError(f"{'<blank>' if not varName else varName} string cannot be empty.")

    return tmpStr


def verify_db_fields(dbFlds: Dict[str, str], dataFormats: typeDefFormats, serviceName: str) -> Dict[str, str]:
    """Verify data fields."""
    if not set(dbFlds.values()).issubset(set(dataFormats.keys())):
        log.error(f"Invalid {serviceName} data formats in data fields")
        raise InvalidAttributeError(
            "Invalid data formats in data fields", service=serviceName
        )

    return dbFlds
