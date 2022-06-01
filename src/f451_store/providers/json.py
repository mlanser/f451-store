"""JSON file storage provider class used in f451 Datastore module.

This module adds an abstraction layer for the CSV storage provider. Its
main purpose is to support CSV file storage.
"""
import json
import logging
import pprint
from pathlib import Path
from typing import Any
from typing import Dict
from typing import List
from typing import Union

import f451_store.constants as const
import f451_store.providers.txt as txt
from f451_store.exceptions import StorageAccessError

# =========================================================
#          G L O B A L S   A N D   H E L P E R S
# =========================================================
SRV_CONFIG_SCTN: str = "f451_json"
SRV_PROVIDER: str = "JSON"

log = logging.getLogger()
pp = pprint.PrettyPrinter(indent=4)

FORMAT_MAP = {
    const.FMT_KWD_STR: str,  # strings (e.g. "some long string")
    const.FMT_KWD_STRIDX: str,  # strings as index (for SQL data stores)
    const.FMT_KWD_INT: int,  # integers (e.g. 1, 2, 3, ... gazillion ... maybe ;-)
    const.FMT_KWD_INTIDX: int,  # integers as index (for SQL data stores)
    const.FMT_KWD_FLOAT: float,  # floats (e.g. 0.1. 0.22, 0.333, ... )
    const.FMT_KWD_BOOL: bool,  # booleans (e.g. True|False, Yes|No, etc.)
}


# =========================================================
#        M A I N   C L A S S   D E F I N I T I O N
# =========================================================
class JSON(txt.BaseTXT):
    """JSON file storage class for f451 Datastore module.

    Use this support class to save data to and retrieve from JSON file storage.

    Attributes:
        dataFields:
            data field map
        fName:
            data storage filename
    """

    def __init__(
        self,
        dataFields: Dict[str, str],
        **kwargs: Any,
    ) -> None:
        super().__init__(
            const.SRV_TYPE_JSON,
            SRV_PROVIDER,
            SRV_CONFIG_SCTN,
            dataFields,
            FORMAT_MAP,
            **kwargs,
        )

    def _write_json(self, inData: List[Dict[str, Any]]) -> None:
        try:
            with Path(self._dbHost).open("w+", newline="") as dbPtr:
                json.dump(inData, dbPtr, ensure_ascii=False)

        except OSError as e:
            log.error(
                f"Unable to access {self.serviceName} data file: '{self._dbHost}'"
            )
            raise StorageAccessError(self._dbHost, service=self.serviceName) from e

    def _process_data(self, inData: List[Any]) -> List[Dict[str, Any]]:
        outData = []

        for row in inData:
            outData.append(self._process_row(row))

        return outData

    def _read_json(self) -> Union[List[Dict[str, Any]], Any]:
        """Read data rows from CSV file.

        This is a common helper function to read data rows.

        Returns:
            'list' of records where each record is represented by a 'dict' object

        Raises:
            StorageAccessError: If unable to read CSV file
        """
        if not Path(self._dbHost).exists():
            return []

        try:
            with Path(self._dbHost).open("r+", newline="") as dbPtr:
                data = json.load(dbPtr)

        except json.JSONDecodeError as e:
            log.error(
                f"Unable to access {self.serviceName} data file: '{self._dbHost}'"
            )
            raise StorageAccessError(self._dbHost, service=self.serviceName) from e

        return data

    def count_records(self, **kwargs: Any) -> int:
        """Count number of records based on set of criteria.

        Args:
            kwargs:
                Additional optional arguments

        Returns:
            Number of records in CSV file.
        """
        return len(self._read_json())

    def store_records(
        self, inData: Union[List[Dict[str, Any]], Dict[str, Any]]
    ) -> None:
        """Store data records in JSON file.

        Wrapper function to store data records.

        Args:
            inData:
                Data to be stored as single 'dict' or 'list' of 'dicts'
        """
        # Merge new and old data if 'append' mode
        oldData = self._process_data(self._read_json()) if self._append else []

        # Write data records to file
        newData = inData if isinstance(inData, list) else [inData]

        self._write_json(oldData + newData)

    def retrieve_records(self, numRecs: int = 1, **kwargs: Any) -> List[Dict[str, Any]]:
        """Retrieve data from CSV file.

        Args:
            numRecs:
                Number of records to retrieve
            kwargs:
                Additional optional arguments
                    - 'newest' - get last/newest records if 'True' else get first/oldest records
                    - alternative: use 'oldest' attrib with opposite value
                    - default is to retrieve last/newest records

                    Note: 'newest' = True == 'oldest' = False
                    Note: if both attribs are defined, then 'newest' value is used

        Returns:
            'list' of records where each record is represented by a 'dict' object.
        """
        jsonData = self._read_json()
        totRecs = len(jsonData)
        newest = kwargs.get(const.KWD_NEWEST, not kwargs.get(const.KWD_OLDEST, False))
        recEnd = totRecs if newest else numRecs
        recStart = max(0, recEnd - numRecs) if newest else 0
        return self._process_data(jsonData[recStart:recEnd])

    def trim_records(self, numRecs: int = 0, **kwargs: Any) -> int:
        """Trim data records from JSON file.

        Args:
            numRecs:
                Number of records to retrieve
            kwargs:
                Additional optional arguments
                    - 'oldest' - trim first/oldest records if 'True' else trim last/newest records
                    - alternative: use 'newest' attrib with opposite value
                    - default is to trim first/oldest records

                    Note: 'oldest' = True == 'newest' = False
                    Note: if both attribs are defined, then 'oldest' value is used

        Returns:
            Number of remaining records.
        """
        jsonData = self._read_json()
        totRecs = len(jsonData)
        oldest = kwargs.get(const.KWD_OLDEST, not kwargs.get(const.KWD_NEWEST, False))
        recStart = numRecs if oldest else 0
        recEnd = totRecs if oldest else max(0, totRecs - numRecs)

        # Write data records to file
        remainingData = jsonData[recStart:recEnd]
        self._write_json(remainingData)
        return len(remainingData)
