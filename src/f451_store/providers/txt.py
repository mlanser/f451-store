"""Default (base) class for text-file based storage f451 Datastore module.

This is a base class for text-file based storage providers (e.g. CSV, JSON,
etc.), and it holds some common methods and attributes used across most/all
such providers/services.
"""
import logging
import pprint
from abc import abstractmethod
from typing import Any
from typing import Callable
from typing import Dict
from typing import List
from typing import Mapping
from typing import Union

import f451_store.constants as const
from f451_store.exceptions import InvalidAttributeError
from f451_store.providers.provider import Provider
from f451_store.providers.provider import verify_file

# =========================================================
#          G L O B A L S   A N D   H E L P E R S
# =========================================================
log = logging.getLogger()
pp = pprint.PrettyPrinter(indent=4)

typeDefFormats = Union[Mapping[str, Callable[[Any], Any]], Dict[str, Any]]


# =========================================================
#        M A I N   C L A S S   D E F I N I T I O N
# =========================================================
class BaseTXT(Provider):
    """Base class for text-file based data storage.

    Attributes:
        serviceType:
            data storage type (e.g. CSV, JSON, SQL, etc.)
        serviceName:
            data storage name (e.g. CSV, JSON, SQLite, PostgreSQL, etc.)
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
            verify_file(
                kwargs.get(const.KWD_DB_HOST, ""),
                not kwargs.get(const.KWD_CREATE, False),
            ),  # 'dbHost' is a filename
            "",  # 'dbPort' is not used for file-based storage
            "",  # 'dbName' is not used for file-based storage
            "",  # 'dbTable' is not used for file-based storage
            "",  # 'dbUserName' is not used for file-based storage
            "",  # 'dbUserPswd' is not used for file-based storage
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
    def formatMap(self) -> typeDefFormats:
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

    def has_table(self, **kwargs: Any) -> bool:
        """Dummy function to ensure consistency with SQL-type storage."""
        log.info(
            f"{self._srvName} is a text-file based service and does not have tables."
        )
        return False

    def _process_row(self, rowIn: Dict[str, str]) -> Dict[str, Any]:
        """Process individual data rows.

        Values are converted according to field and format maps.

        Args:
            rowIn:
                single row of data as a 'dict'

        Returns:
            'dict' object where values have been converted from strings to proper format as per format map.
        """
        rowOut = {}
        for key, val in rowIn.items():
            # Only process/convert field values where
            # we have a known/valid format mapping
            if key in self._dataFields:
                rowOut.update({key: self._dataFormats[self._dataFields[key]](val)})

        return rowOut


# =========================================================
#              U T I L I T Y   F U N C T I O N S
# =========================================================
def verify_db_fields(
    dbFlds: Dict[str, str], dataFormats: typeDefFormats, serviceName: str
) -> Dict[str, str]:
    """Verify data fields."""
    if not set(dbFlds.values()).issubset(set(dataFormats.keys())):
        log.error(f"Invalid {serviceName} data formats in data fields")
        raise InvalidAttributeError(
            "Invalid data formats in data fields", service=serviceName
        )

    return dbFlds
