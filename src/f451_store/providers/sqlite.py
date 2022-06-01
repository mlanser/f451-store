"""SQLite storage provider f451 Datastore module.

This module adds an abstraction layer for the SQLite storage provider. Its
main purpose is to support SQLite storage.
"""
import logging
import pprint
import sqlite3
from pathlib import Path
from typing import Any
from typing import Callable
from typing import Dict
from typing import List
from typing import Mapping
from typing import Optional
from typing import Tuple
from typing import Union

import f451_store.constants as const
import f451_store.providers.sql as sql
from f451_store.exceptions import InvalidAttributeError
from f451_store.exceptions import StorageAccessError
from f451_store.exceptions import StorageConnectionError
from f451_store.providers.provider import verify_file

# =========================================================
#          G L O B A L S   A N D   H E L P E R S
# =========================================================
SRV_CONFIG_SCTN: str = "f451_sqlite"
SRV_PROVIDER: str = "SQLite"

log = logging.getLogger()
pp = pprint.PrettyPrinter(indent=4)

FORMAT_MAP = {
    const.FMT_KWD_STR: "TEXT",  # strings (e.g. "some long string")
    const.FMT_KWD_STRIDX: "TEXT|idx",  # strings as index (for SQL data stores)
    const.FMT_KWD_INT: "INTEGER",  # integers (e.g. 1, 2, 3, ... gazillion ... maybe ;-)
    const.FMT_KWD_INTIDX: "INTEGER|idx",  # integers as index (for SQL data stores)
    const.FMT_KWD_FLOAT: "REAL",  # floats (e.g. 0.1. 0.22, 0.333, ... )
    const.FMT_KWD_BOOL: "NUMERIC",  # booleans (e.g. True|False, Yes|No, etc.)
}

typeDefConnector = Union[sqlite3.Connection, None]
typeDefData = Union[List[Dict[str, Any]], Dict[str, Any]]


# =========================================================
#        M A I N   C L A S S   D E F I N I T I O N
# =========================================================
class SQLite(sql.BaseSQL):
    """SQLite storage class for f451 Datastore module.

    Use this support class to save data to and retrieve from SQLite storage.

    Attributes:
        dataFields:
            data field map
        dbName:
            db filename
        dbTable:
            db table name
    """

    def __init__(
        self,
        dataFields: Dict[str, str],
        **kwargs: Any,
    ) -> None:
        super().__init__(
            const.SRV_TYPE_SQL,
            SRV_PROVIDER,
            SRV_CONFIG_SCTN,
            dataFields,
            FORMAT_MAP,
            **kwargs,
        )
        self._dbConn: typeDefConnector = None

    @property
    def isConnectionOpen(self) -> bool:
        """Return 'dbConn' property."""
        return self._dbConn is not None

    def connect_open(self, force: Optional[bool] = False) -> typeDefConnector:
        """Establish connection to SQLite database.

        Args:
            force:
                If 'True' then we close any open connection and create new connection

        Returns:
            'sqlite3.Connection' object

        Raises:
            StorageConnectionError: If unable to establish connection to SQLite database
        """
        if force and self._dbConn is not None:
            self._dbConn.close()
            self._dbConn = None

        if self._dbConn is None:
            try:
                self._dbConn = sqlite3.connect(self._dbHost)

            except sqlite3.Error as e:
                log.error(
                    f"Unable to access {SRV_PROVIDER} database: '{str(self._dbHost)}'"
                )
                raise StorageConnectionError(
                    message=f"Unable to connect to SQLite DB: '{self._dbHost}'",
                    errors=[str(e)],
                ) from e

        return self._dbConn

    def connect_close(self, force: Optional[bool] = True) -> None:
        """Close connection to SQLite database.

        Args:
            force:
                If 'True' then we close any open connection and create new connection
        """
        if force and self._dbConn is not None:
            self._dbConn.close()
            self._dbConn = None

    def _has_table_helper(self, dbTable: Optional[str] = None) -> bool:
        """Helper method to check if given database table exists.

        SQLIte3 stores table names in the 'sqlite_master' table.

        Args:
            dbTable:
                Name of database table

        Returns:
            'True' if table exists.

        Raises:
            StorageAccessError: If database table cannot be verified.
        """
        tblName = self._dbTable if not dbTable else dbTable
        dbConn = self.connect_open()

        if self._dbConn is None:
            log.error(f"Unable to connect to {SRV_PROVIDER} database: '{tblName}'")
            raise StorageAccessError(SRV_PROVIDER)

        dbCur = dbConn.cursor()  # type: ignore[union-attr]
        try:
            dbCur.execute(
                f"SELECT count(name) FROM sqlite_master WHERE type='table' AND name='{tblName}';"  # noqa: S608
            )
        except sqlite3.Error as e:
            log.error(f"Unable to verify table in {SRV_PROVIDER} database: '{tblName}'")
            self.connect_close(True)
            raise StorageAccessError(SRV_PROVIDER) from e

        return int(dbCur.fetchone()[0]) == 1

    def _create_table_helper(
        self,
        dbTable: Optional[str] = None,
        dataFields: Optional[Dict[str, str]] = None,
        formatMap: Optional[Dict[str, str]] = None,
    ) -> None:
        """Helper method to create a new table in an SQLite database.

        Args:
            dbTable:
                Name of database table
            dataFields:
                Data fields
            formatMap:
                Data field format map

        Raises:
            InvalidAttributeError: If database field maps are invalid.
            StorageAccessError: If database table cannot be created.
        """
        tblName = self._dbTable if not dbTable else dbTable
        dtaFlds = self._dataFields if not dataFields else dataFields
        fmtMap = self._dataFormats if not formatMap else formatMap
        dbConn = self.connect_open()

        if self._dbConn is None:
            log.error(f"Unable to connect to {SRV_PROVIDER} database: '{tblName}'")
            raise StorageAccessError(SRV_PROVIDER)

        dbCur = dbConn.cursor()  # type: ignore[union-attr]

        try:
            newFlds = [
                f"{str(key)} {self._split_type_idx(str(fmtMap[val]))[0]}"
                for (key, val) in dtaFlds.items()
            ]
            dbCur.execute(
                f"CREATE TABLE IF NOT EXISTS {tblName} ({','.join(newFlds)});"
            )

            # SQLite automatically creates a 'primary key' column, and we'll therefore
            # only create indexed columns as indicated in 'fldNamesWithTypes'.
            self._create_indexed_columns(dbCur, dtaFlds, fmtMap, tblName)

        except KeyError as e:
            log.error(f"Invalid data format: '{str(e)}'")
            self.connect_close(True)
            raise InvalidAttributeError(str(e), service=SRV_PROVIDER) from e

        except sqlite3.Error as e:
            log.error(f"Unable to create table in {SRV_PROVIDER} database: '{tblName}'")
            self.connect_close(True)
            raise StorageAccessError(SRV_PROVIDER) from e

    def _create_indexed_columns(
        self,
        dbCur: sqlite3.Cursor,
        dtaFlds: Dict[str, str],
        fmtMap: Union[Mapping[str, Callable[[Any], Any]], Dict[str, str]],
        tblName: str,
    ) -> None:
        """Create indexed columns."""
        try:
            for (key, val) in dtaFlds.items():
                if self._split_type_idx(str(fmtMap[val]))[1]:
                    dbCur.execute(
                        f"CREATE INDEX idx_{tblName}_{key} ON {tblName}({key});"
                    )

        except sqlite3.Error as e:
            log.error(
                f"Unable to create table index in {SRV_PROVIDER} database: '{tblName}'"
            )
            self.connect_close(True)
            raise StorageAccessError(SRV_PROVIDER) from e

    @staticmethod
    def _split_type_idx(inStr: str) -> Tuple[str, bool]:
        parts = inStr.split("|")
        if len(parts) > 1:
            return parts[0], (parts[1].lower() == "idx")

        return parts[0], False

    def _store_records_helper(
        self,
        data: List[Dict[str, Any]],
        dbTable: Optional[str] = None,
        dataFields: Optional[Dict[str, str]] = None,
    ) -> None:
        """Helper method to store data records in an SQLite database.

        Args:
            data:
                Data to be stored as single 'dict' or 'list' of 'dicts'
            dbTable:
                Name of database table
            dataFields:
                Data fields

        Raises:
            StorageAccessError: If database table cannot be verified.
        """
        tblName = self._dbTable if not dbTable else dbTable
        dtaFlds = self._dataFields if not dataFields else dataFields
        dbConn = self.connect_open()

        if self._dbConn is None:
            log.error(f"Unable to connect to {SRV_PROVIDER} database: '{tblName}'")
            raise StorageAccessError(SRV_PROVIDER)

        dbCur = dbConn.cursor()  # type: ignore[union-attr]

        try:
            # Filter each row to only hold approved keys using dictionary
            # comprehension and, of course, a common set of keys ;-)
            validFlds = list(set(data[0].keys()) & set(dtaFlds.keys()))
            if len(validFlds) > 0:
                flds = ",".join(validFlds)
                vals = ",".join("?" for _ in validFlds)
                for row in data:
                    # Using list comprehension to only pull values
                    # that we want/need from a row of data
                    dbCur.execute(
                        f"INSERT INTO {tblName}({flds}) VALUES({vals})",
                        [row[key] for key in validFlds],
                    )

        except sqlite3.Error as e:
            log.error(
                f"Unable to store records in {SRV_PROVIDER} database: '{self._dbTable}'"
            )
            self.connect_close(True)
            raise StorageAccessError(SRV_PROVIDER) from e

    def _count_records_helper(self, dbTable: Optional[str] = None) -> int:
        """Helper method to count number of records in an SQLite database.

        Args:
            dbTable:
                Name of database table

        Returns:
            Number of records in a table

        Raises:
            StorageAccessError: If database table cannot be verified.
        """
        tblName = self._dbTable if not dbTable else dbTable
        dbConn = self.connect_open()

        if self._dbConn is None:
            log.error(f"Unable to connect to {SRV_PROVIDER} database: '{tblName}'")
            raise StorageAccessError(SRV_PROVIDER)

        dbCur = dbConn.cursor()  # type: ignore[union-attr]

        if not self._has_table_helper(tblName):
            return 0

        numRecs = 0
        try:
            dbCur.execute(f"SELECT COUNT(*) FROM {tblName};")  # noqa: S608
            numRecs = dbCur.fetchone()[0]

        except sqlite3.Error as e:
            log.error(
                f"Unable to get record count from {SRV_PROVIDER} database: '{tblName}'"
            )
            self.connect_close(True)
            raise StorageAccessError(SRV_PROVIDER) from e

        return numRecs

    def _retrieve_records_helper(
        self,
        numRecs: int,
        newest: Optional[bool] = True,
        orderBy: Optional[str] = None,
        dbTable: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Helper method to retrieve records from an SQLite database.

        Args:
            numRecs:
                Number of records to retrieve
            newest:
                Select 'newest'/last items in sorted list
            orderBy:
                Name of data field to sort/order records by before selection
            dbTable:
                Name of database table

        Returns:
            List of selected records from a table

        Raises:
            StorageAccessError: If database table cannot be verified.
        """
        tblName = self._dbTable if not dbTable else dbTable
        fldNames = self._dataFields.keys()
        flds = ",".join(f"{key}" for key in fldNames)
        sortFld = list(fldNames)[0] if orderBy is None else orderBy
        dbConn = self.connect_open()

        if self._dbConn is None:
            log.error(f"Unable to connect to {SRV_PROVIDER} database: '{tblName}'")
            raise StorageAccessError(SRV_PROVIDER)

        dbCur = dbConn.cursor()  # type: ignore[union-attr]

        try:
            if newest:
                # Get 'newest' records -- we're not using f-strings here to improve readability
                dbCur.execute(
                    "SELECT * FROM (SELECT {flds} FROM {tbl} {inner} LIMIT {limit}) {order}".format(  # noqa: S608
                        flds=flds,
                        tbl=tblName,
                        inner=create_orderby_param(sortFld, True),
                        limit=numRecs,
                        order=create_orderby_param(sortFld),
                    )
                )
            else:
                # Get 'oldest' records -- we're not using f-strings here to improve readability
                dbCur.execute(
                    "SELECT {flds} FROM {tbl} {order} LIMIT {limit}".format(  # noqa: S608
                        flds=flds,
                        tbl=tblName,
                        order=create_orderby_param(sortFld),
                        limit=numRecs,
                    )
                )

            dataRecords = dbCur.fetchall()

        except sqlite3.Error as e:
            log.error(
                f"Failed to retrieve data from {SRV_PROVIDER} database: '{tblName}'"
            )
            self.connect_close(True)
            raise StorageAccessError(SRV_PROVIDER) from e

        data = []
        for row in dataRecords:
            # Create dictionary with keys from field name
            # list, mapped against values from database.
            data.append(dict(zip(fldNames, row)))

        return data

    def _trim_records_helper(
        self,
        numRecs: int,
        newest: Optional[bool] = True,
        orderBy: Optional[str] = None,
        dbTable: Optional[str] = None,
    ) -> int:
        """Helper method to trim records from an SQLite database.

        This is essentially the opposite of retrieving records.

        Args:
            numRecs:
                Number of records to trim
            newest:
                Select 'newest'/last items in sorted list
            orderBy:
                Name of data field to sort/order records by before selection
            dbTable:
                Name of database table

        Returns:
            Number of deleted records

        Raises:
            StorageAccessError: If database table cannot be verified.
        """
        tblName = self._dbTable if not dbTable else dbTable
        fldNames = self._dataFields.keys()
        sortFld = list(fldNames)[0] if orderBy is None else orderBy
        dbConn = self.connect_open()

        if self._dbConn is None:
            log.error(f"Unable to connect to {SRV_PROVIDER} database: '{tblName}'")
            raise StorageAccessError(SRV_PROVIDER)

        dbCur = dbConn.cursor()  # type: ignore[union-attr]

        foundRecs = self._retrieve_records_helper(numRecs, newest, orderBy)
        delRecs = [foundRecs[i][sortFld] for i, val in enumerate(foundRecs)]

        try:
            for i in delRecs:
                dbCur.execute(
                    "DELETE FROM {tbl} WHERE {fld} = {val}".format(  # noqa: S608
                        tbl=tblName, fld=sortFld, val="'i'" if isinstance(i, str) else i
                    )
                )

        except sqlite3.Error as e:
            log.error(f"Failed to trim data from {SRV_PROVIDER} database: '{tblName}'")
            self.connect_close(True)
            raise StorageAccessError(SRV_PROVIDER) from e

        return len(delRecs)

    def has_table(self, dbTable: str, **kwargs: Any) -> bool:
        """Public method to check if given table exists in current database.

        Args:
            dbTable:
                Name of database table
            kwargs:
                - 'close' -- close DB connection if 'True'

        Returns:
            'True' if table exists.
        """
        closeConn = kwargs.get(const.KWD_CLOSE, True)
        is_valid = self._has_table_helper(dbTable)
        self.connect_close(closeConn)
        return is_valid

    def create_table(self, dbTable: str, **kwargs: Any) -> None:
        """Public method to create a new table with given set of fields.

        Args:
            dbTable:
                Name of database table
            kwargs:
                - 'close' -- close DB connection if 'True'
        """
        closeConn = kwargs.get(const.KWD_CLOSE, True)
        dbConn = self.connect_open()

        if not self._has_table_helper(dbTable):
            self._create_table_helper(dbTable)
            dbConn.commit()  # type: ignore[union-attr]

        self.connect_close(closeConn)

    def store_records(self, inData: typeDefData, **kwargs: Any) -> None:
        """Store data records in SQLite database.

        Args:
            inData:
                Data to be stored as single 'dict' or 'list' of 'dicts'
            kwargs:
                - 'close' -- close DB connection if 'True'
        """
        closeConn = kwargs.get(const.KWD_CLOSE, True)
        dbConn = self.connect_open()

        if not self._has_table_helper():
            self._create_table_helper()
            dbConn.commit()  # type: ignore[union-attr]

        # Ensure that we always handle a list of rows
        data = inData if isinstance(inData, list) else [inData]

        self._store_records_helper(data)
        dbConn.commit()  # type: ignore[union-attr]

        self.connect_close(closeConn)

    def count_records(self, **kwargs: Any) -> int:
        """Count number of records in SQLite database.

        Args:
            kwargs:
                'close' -- close DB connection if 'True'

        Returns:
            Number of records in a table
        """
        closeConn = kwargs.get(const.KWD_CLOSE, True)

        numRecs = self._count_records_helper()

        self.connect_close(closeConn)
        return numRecs

    def retrieve_records(self, numRecs: int = 1, **kwargs: Any) -> List[Dict[str, Any]]:
        """Retrieve data records from SQLite database.

        Args:
            numRecs:
                Number of records to retrieve
            kwargs:
                'close'    -- close DB connection if 'True'
                'order_by' -- Field to sorted by
                'first'    -- If TRUE, retrieve first 'numRecs' records.
                              Else retrieve last 'numRecs' records.

        Returns:
            List of records
        """
        orderBy = kwargs.get(const.KWD_ORDER_BY, None)
        newest = kwargs.get(const.KWD_NEWEST, True)
        closeConn = kwargs.get(const.KWD_CLOSE, True)

        foundRecs = self._retrieve_records_helper(numRecs, newest, orderBy)

        self.connect_close(closeConn)
        return foundRecs

    def trim_records(self, numRecs: int = 0, **kwargs: Any) -> int:
        """Trim data records from SQLite database.

        Args:
            numRecs:
                Number of records to trim
            kwargs:
                'order_by'  - Field to sorted by
                'newest'    - If TRUE, retrieve first 'numRecs' records.
                              Else retrieve last 'numRecs' records.

        Returns:
            List of records
        """
        orderBy = kwargs.get(const.KWD_ORDER_BY, None)
        newest = kwargs.get(const.KWD_NEWEST, True)

        closeConn = kwargs.get(const.KWD_CLOSE, True)

        trimRecs = self._trim_records_helper(numRecs, newest, orderBy)

        self.connect_close(closeConn)
        return trimRecs


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

    Returns:
        New 'ORDER BY' string to be used in SQL query
    """

    def _flip_orderby(s: str, flg: bool = False) -> str:
        if s == "ASC":
            return "ASC" if not flg else "DESC"

        return "DESC" if not flg else "ASC"

    parts = inStr.split("|")
    if len(parts) < 1:
        return ""

    outStr = "ASC" if len(parts) == 1 else parts[1].upper()
    return f"ORDER BY {parts[0]} {_flip_orderby(outStr, flip)}"


def verify_db_file(
    dbHost: Union[Path, str], serviceName: str, strict: bool
) -> Union[Path, str]:
    """Verify that database file exists.

    This method is really designed to verify that a SQLite database file exists. However, SQLite
    also supports an in-memory mode which does not have an actual file associated, and we need
    to check for that first.

    Args:
        dbHost:
            Name of database file which can be an actual file name, or the ':memory:' keyword.
        serviceName:
            Name of service (should be 'SQLite')
        strict:
            If 'True' then we require that file/hosts exists.

    Returns:
        Path/file/name of database.
    """
    if serviceName == const.STORAGE_SQLITE:
        return (
            const.KWD_IN_MEMORY
            if (str(dbHost).lower() == const.KWD_IN_MEMORY)
            else verify_file(str(dbHost), strict)
        )

    # Houston, we have a problem!
    return ""
