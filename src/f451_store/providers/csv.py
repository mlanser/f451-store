"""CSV file storage provider class used in f451 Datastore module.

This module adds an abstraction layer for the CSV storage provider. Its
main purpose is to support CSV file storage.
"""
import logging
import pprint
from csv import DictReader
from csv import DictWriter
from csv import Error as csvError
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
SRV_CONFIG_SCTN: str = "f451_csv"
SRV_PROVIDER: str = "CSV"

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
class CSV(txt.BaseTXT):
    """CSV file storage class for f451 Datastore module.

    Use this support class to save data to and retrieve from CSV file storage.

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
            const.SRV_TYPE_CSV,
            SRV_PROVIDER,
            SRV_CONFIG_SCTN,
            dataFields,
            FORMAT_MAP,
            **kwargs,
        )

    def count_records(self, **kwargs: Any) -> int:
        """Count number of records based on set of criteria.

        Args:
            kwargs:
                Additional optional arguments

        Returns:
            Number of records in CSV file.

        Raises:
            StorageAccessError: If unable to read CSV file
        """
        try:
            with Path(self._dbHost).open("r", newline="") as dbPtr:
                cntr = sum(1 for _ in dbPtr)  # Count lines ...
                dbPtr.seek(0)  # ... and 'rewind' file to beginning
        except csvError as e:
            log.error(
                f"Unable to access {self.serviceName} data file: '{str(self._dbHost)}'"
            )
            raise StorageAccessError(str(self._dbHost), service=self.serviceName) from e

        return cntr - 1 if cntr > 1 else 0  # We don't count 'header' row

    def _write_header_row(self, mode: str = "w+") -> None:
        """Write header row to CSV file.

        This is a common task that is used by several methods in the CSV class.

        Args:
            mode:
                File open 'mode' string

        Raises:
            StorageAccessError: If unable to write to CSV file
        """
        try:
            with Path(self._dbHost).open(mode, newline="") as dbPtr:
                csvWriter = DictWriter(dbPtr, fieldnames=self._dataFields.keys())
                csvWriter.writeheader()
        except csvError as e:
            log.error(
                f"Unable to access {self.serviceName} data file: '{self._dbHost}'"
            )
            raise StorageAccessError(self._dbHost, service=self.serviceName) from e

    def _write_data_records(
        self, inData: List[Dict[str, Any]], mode: str = "a+"
    ) -> None:
        """Write data records to CSV file.

        This is a common task that is used by several methods in the CSV class.

        Args:
            inData:
                List of data rows. Each data row is a 'dict'
            mode:
                File open 'mode' string

        Raises:
            StorageAccessError: If unable to write to CSV file
        """
        try:
            with Path(self._dbHost).open(mode, newline="") as dbPtr:
                csvWriter = DictWriter(dbPtr, fieldnames=self._dataFields.keys())
                csvWriter.writerows(inData)
        except csvError as e:
            log.error(
                f"Unable to access {self.serviceName} data file: '{self._dbHost}'"
            )
            raise StorageAccessError(self._dbHost, service=self.serviceName) from e

    def store_records(
        self, inData: Union[List[Dict[str, Any]], Dict[str, Any]]
    ) -> None:
        """Store data records in CSV file.

        Wrapper function to store data records. This function also writes the
        header row as needed.

        Args:
            inData:
                Data to be stored as single 'dict' or 'list' of 'dicts'
        """
        data = inData if isinstance(inData, list) else [inData]
        dbFile = Path(self._dbHost)

        # Write header row if not in 'append' mode, when file does not exist, or file is empty
        if not self._append or not dbFile.exists() or not dbFile.stat().st_size:
            self._write_header_row("w+")

        # Write data records to file
        self._write_data_records(data, "a+")

    def _read_data_rows(
        self, recStart: int, recEnd: int, process: bool = True
    ) -> List[Dict[str, Any]]:
        """Read data rows from CSV file.

        This is a common helper function to read data rows.

        Args:
            recStart:
                first record to retrieve
            recEnd:
                last record to retrieve
            process:
                if 'True' then convert record values from strings to proper formats as per format map.

        Returns:
            'list' of records where each record is represented by a 'dict' object

        Raises:
            StorageAccessError: If unable to read CSV file
        """
        data = []
        try:
            with Path(self._dbHost).open("r", newline="") as dbPtr:
                csvReader = DictReader(dbPtr)
                for i, row in enumerate(csvReader, 0):
                    if i < recStart:
                        continue
                    elif i > recEnd:
                        break
                    else:
                        data.append(self._process_row(row) if process else row)

        except csvError as e:
            log.error(
                f"Unable to access {self.serviceName} data file: '{self._dbHost}'"
            )
            raise StorageAccessError(self._dbHost, service=self.serviceName) from e

        return data

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
        totRecs = self.count_records()
        newest = kwargs.get(const.KWD_NEWEST, not kwargs.get(const.KWD_OLDEST, False))
        recEnd = totRecs if newest else min(totRecs, numRecs - 1)
        recStart = max(0, recEnd - numRecs) if newest else 0

        return self._read_data_rows(recStart, recEnd)

    def trim_records(self, numRecs: int = 0, **kwargs: Any) -> int:
        """Trim data records from CSV file.

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
        totRecs = self.count_records()
        oldest = kwargs.get(const.KWD_OLDEST, not kwargs.get(const.KWD_NEWEST, False))
        recStart = numRecs if oldest else 0
        recEnd = totRecs if oldest else max(0, totRecs - numRecs - 1)

        data = (
            [] if numRecs > totRecs else self._read_data_rows(recStart, recEnd, False)
        )

        self._write_header_row("w+")
        self._write_data_records(data, "a+")

        return len(data)


# =========================================================
#              U T I L I T Y   F U N C T I O N S
# =========================================================
