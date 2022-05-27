"""MySQL storage provider f451 Datastore module.

This module adds an abstraction layer for the MySQL storage provider. Its
main purpose is to support MySQL storage.
"""
import logging
import pprint
import mysql.connector
from pathlib import Path
from typing import Any
from typing import Optional
from typing import Dict
from typing import List
from typing import Mapping
from typing import Union

import f451_store.constants as const
from f451_store.exceptions import InvalidAttributeError
from f451_store.exceptions import StorageConnectionError
from f451_store.exceptions import StorageAccessError
import f451_store.providers.sql as sql
from f451_store.providers.provider import verify_file

# =========================================================
#          G L O B A L S   A N D   H E L P E R S
# =========================================================
SRV_CONFIG_SCTN: str = "f451_mysql"
SRV_PROVIDER: str = "MySQL"

log = logging.getLogger()
pp = pprint.PrettyPrinter(indent=4)

FORMAT_MAP = {
    const.FMT_KWD_STR: 'TEXT',              # strings (e.g. "some long string")
    const.FMT_KWD_STRIDX: 'TEXT|idx',       # strings as index (for SQL data stores)
    const.FMT_KWD_INT: 'INTEGER',           # integers (e.g. 1, 2, 3, ... gazillion ... maybe ;-)
    const.FMT_KWD_INTIDX: 'INTEGER|idx',    # integers as index (for SQL data stores)
    const.FMT_KWD_FLOAT: 'REAL',            # floats (e.g. 0.1. 0.22, 0.333, ... )
    const.FMT_KWD_BOOL: 'NUMERIC',          # booleans (e.g. True|False, Yes|No, etc.)
}


# =========================================================
#        M A I N   C L A S S   D E F I N I T I O N
# =========================================================
class MySQL(sql.BaseSQL):
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
        dbName: Path,
        dbTable: str,
        **kwargs: Any,
    ) -> None:
        super().__init__(
            const.SRV_TYPE_SQL,
            SRV_PROVIDER,
            SRV_CONFIG_SCTN,
            dataFields,
            FORMAT_MAP,
            dbName,
            dbTable,
            **kwargs,
        )
        self._create: bool = kwargs.get(const.KWD_CREATE, False)
        self._dbConn: Any = None
        self.dbUserName = kwargs.get(const.KWD_DB_USER_NAME, '')
        self.dbUserPswd = kwargs.get(const.KWD_DB_USER_PSWD, '')

    @property
    def dbUserName(self) -> str:
        """Return 'dbUserName' property."""
        return self._dbUserName

    @dbUserName.setter
    def dbUserName(self, inName: str) -> None:
        """Set 'dbUserName' property."""
        self._dbUserName = inName

    @property
    def dbUserPswd(self) -> str:
        """Return 'dbUserPswd' property."""
        return self._dbUserPswd

    @dbUserPswd.setter
    def dbUserPswd(self, inPswd: str) -> None:
        """Set 'dbUserPswd' property."""
        self._dbUserPswd = inPswd

    def _connect_to_db(self) -> Any:
        """Establish connection to MySQL database.

        Returns:
            MySQL Connection object

        Raises:
            StorageConnectionError: If unable to establish connection to SQLite database
        """
        self._dbConn = None
        try:
            self._dbConn = mysql.connector.connect(
                host=self._db,
                user=self._dbUserName,
                passwd=self._dbUserPswd,
            )

        except mysql.connector.Error as e:
            log.error(f"Unable to access {SRV_PROVIDER} database: '{str(self._db)}'")
            raise StorageConnectionError(message=f"Unable to connect to SQLite DB: '{self._db}'", errors=[str(e)]) from e

        return self._dbConn

    @staticmethod
    def _exist_table(dbCur: sqlite3.Cursor, dbTable: str) -> bool:
        """Helper method to check if given database table exists.

        SQLIte3 stores table names in the 'sqlite_master' table.

        Args:
            dbCur:
                DB cursor for current database connection
            dbTable:
                Name of database table

        Returns:
            'True' if table exists.

        Raises:
            StorageAccessError: If database table cannot be verified.
        """
        try:
            qry = f"SELECT count(name) FROM sqlite_master WHERE type='table' AND name='{dbTable}'"
            print(f"{qry =}")
            dbCur.execute(qry)
            # dbCur.execute(f"SELECT count(name) FROM sqlite_master WHERE type='table' AND name='{dbTable}'")
        except sqlite3.Error as e:
            log.error(f"Unable to verify table in {SRV_PROVIDER} database: '{dbTable}'")
            raise StorageAccessError(SRV_PROVIDER) from e

        return dbCur.fetchone()[0] == 1

    def _make_table(
            self,
            dbCur: sqlite3.Cursor,
            dbTable: Optional[str],
            dataFields: Optional[Dict[str, str]],
            formatMap: Optional[Dict[str, str]]
    ) -> None:
        """Helper method to make a new table in a given database.

        Args:
            dbCur:
                DB cursor for current database connection
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

        def _split_type_idx(inStr):
            parts = inStr.split('|')
            if len(parts) > 1:
                return parts[0], (parts[1].lower() == 'idx')

            return parts[0], False

        tblName = self._dbTable if not dbTable else dbTable
        dtaFlds = self._dataFields if not dataFields else dataFields
        fmtMap = self._dataFormats if not formatMap else formatMap

        print(f"CREATE TABLE: {tblName}")
        try:
            newFlds = [
                f"{str(key)} {str(_split_type_idx(fmtMap[val])[0])}"
                for (key, val) in dtaFlds.items()
            ]
            qry = f"CREATE TABLE IF NOT EXISTS {tblName} ({','.join(newFlds)});"
            print(f"{qry =}")
            dbCur.execute(qry)

        except KeyError as e:
            log.error(f"Invalid data format: '{str(e)}'")
            raise InvalidAttributeError(str(e), service=SRV_PROVIDER) from e

        except sqlite3.Error as e:
            log.error(f"Unable to create table in {SRV_PROVIDER} database: '{tblName}'")
            raise StorageAccessError(SRV_PROVIDER) from e

        # SQLite automatically creates a 'primary key' column, and we'll therefore
        # only create indexed columns as indicated in 'fldNamesWithTypes'.
        try:
            for (key, val) in dtaFlds.items():
                if _split_type_idx(fmtMap[val])[1]:
                    qry = f"CREATE INDEX idx_{tblName}_{key} ON {tblName}({key});"
                    print(f"{qry =}")
                    dbCur.execute(qry)

        except sqlite3.Error as e:
            log.error(f"Unable to create table index in {SRV_PROVIDER} database: '{tblName}'")
            raise StorageAccessError(SRV_PROVIDER) from e

    def create_table(self, dbTable: str, dataFields: Dict[str, str], formatMap: Dict[str, str]) -> None:
        """Public method to create a new table with given set of fields.

        Args:
            dbTable:
                Name of database table
            dataFields:
                Data fields
            formatMap:
                Data field format map
        """
        dbConn = self._connect_to_db()
        dbCur = dbConn.cursor()

        if not self._exist_table(dbCur, dbTable):
            self._make_table(dbCur, dbTable, dataFields, formatMap)

        dbConn.commit()
        dbConn.close()

    def store_records(
        self, inData: Union[List[Dict[str, Any]], Dict[str, Any]]
    ) -> None:
        """Store data records in SQLite database.

        Args:
            inData:
                Data to be stored as single 'dict' or 'list' of 'dicts'

        Raises:
            StorageAccessError: If database table cannot be verified.
        """
        dbConn = connect_to_db(self._db)
        dbCur = dbConn.cursor()

        # Ensure that we always handle a list of rows
        data = inData if isinstance(inData, list) else [inData]

        try:
            # Filter each row to only hold approved keys using dictionary
            # comprehension and, of course, a common set of keys ;-)
            for row in data:
                validFlds = list(set(row.keys()) & set(self._dataFields.keys()))

                if len(validFlds) > 0:
                    flds = ','.join(validFlds)
                    vals = ','.join("?" for _ in validFlds)
                    # Using list comprehension to only pull values
                    # that we want/need from a row of data
                    dbCur.execute(f"INSERT INTO {self._dbTable}({flds}) VALUES({vals})",
                                  [row[key] for key in validFlds])

        except sqlite3.Error as e:
            log.error(f"Unable to store records in {SRV_PROVIDER} database: '{self._dbTable}'")
            raise StorageAccessError(SRV_PROVIDER) from e

        dbConn.commit()
        dbConn.close()

    def retrieve_records(self, numRecs: int = 1, **kwargs: Any) -> List[Dict[str, Any]]:
        # """Retrieve data records from SQLite database.
        #
        # Args:
        #     numRecs:  Number of records to retrieve
        #     attribs:
        #         'orderBy'   - Field to sorted by
        #         'first'     - If TRUE, retrieve first 'numRecs' records.
        #                       Else retrieve last 'numRecs' records.
        # Returns:
        #     list:     List of all records retrieved
        #
        # Raises:
        #     OSError: If unable to access or read data from SQLite database.
        # """
        # orderBy = self._parse_attribs(attribs, 'orderBy', None)
        # first = self._parse_attribs(attribs, 'first', True)
        #
        # dbConn = self._connect_server()
        # dbCur = dbConn.cursor()
        #
        # fldNames = self._flds.keys()
        # flds = ','.join("{!s}".format(key) for key in fldNames)
        # sortFld = list(fldNames)[0] if orderBy is None else orderBy
        #
        # try:
        #     if first:
        #         dbCur.execute('SELECT {flds} FROM {tbl} {order} LIMIT {limit}'.format(
        #             flds=flds,
        #             tbl=self._table,
        #             order=self._create_orderby_param(sortFld),
        #             limit=numRecs
        #         ))
        #     else:
        #         dbCur.execute('SELECT * FROM (SELECT {flds} FROM {tbl} {inner} LIMIT {limit}) {order}'.format(
        #             flds=flds,
        #             tbl=self._table,
        #             inner=self._create_orderby_param(sortFld, True),
        #             limit=numRecs,
        #             order=self._create_orderby_param(sortFld)
        #         ))
        #
        #     dataRecords = dbCur.fetchall()
        #
        # except sqlite3.Error as e:
        #     raise OSError("Failed to retrieve data from SQLite database '{}'\n{}!".format(self._name, e))
        #
        # dbConn.close()
        #
        # data = []
        # for row in dataRecords:
        #     # Create dictionary with keys from field name
        #     # list, mapped against values from database.
        #     data.append(dict(zip(self._flds.keys(), row)))
        #
        # return data
        pass

    def trim_records(self, numRecs: int = 0, **kwargs: Any) -> int:
        pass

    def count_records(self, **kwargs: Any) -> int:
        """Count number of records in SQLite database.

        Returns:
            Number of records in a table

        Raises:
            StorageAccessError: If database table cannot be verified.
        """
        dbConn = connect_to_db(self._db)
        dbCur = dbConn.cursor()

        try:
            dbCur.execute(f"SELECT COUNT(*) FROM {self._dbTable}")
            numRecs = dbCur.fetchone()

        except sqlite3.Error as e:
            log.error(f"Unable to get record count from {SRV_PROVIDER} database: '{self._dbTable}'")
            raise StorageAccessError(SRV_PROVIDER) from e

        dbConn.close()
        return numRecs[0]

    def has_table(self, dbTable: str) -> bool:
        """Public method to check if given table exists in current database.

        Args:
            dbTable:
                Name of database table

        Returns:
            'True' if table exists.
        """
        dbConn = self._connect_to_db()
        # print(f"Checking: {self._dbTable if not dbTable else dbTable}")
        is_valid = self._exist_table(dbConn.cursor(), dbTable)
        dbConn.close()
        return is_valid

    # @staticmethod
    # def _count_records_helper(dbCur: sqlite3.Cursor, dbTable: str):
    #     dbCur.execute(f"SELECT COUNT(*) FROM {dbTable}")
    #     numRecs = dbCur.fetchone()
    #
    #     return numRecs[0]

    # def total_records(self):
    #     """Count number of records in SQLite database.
    #
    #     Raises:
    #         OSError: If unable to access or read data from SQLite database.
    #     """
    #     dbConn = self._connect_server()
    #     dbCur = dbConn.cursor()
    #
    #     try:
    #         numRecs = self._count_records(dbCur, self._table)
    #
    #     except sqlite3.Error as e:
    #         raise OSError("Failed to retrieve data from SQLite database '{}'\n{}!".format(self._name, e))
    #
    #     dbConn.close()
    #
    #     return numRecs

    # def store_data(self, data=None):
    #     """Save data to SQLite database.
    #
    #     Args:
    #         data:    List with one or more data rows
    #
    #     Raises:
    #         OSError: If unable to access or save data to JSON file.
    #     """
    #     dbConn = self._connect_server()
    #     dbCur = dbConn.cursor()
    #
    #     if not self._exist_table(dbCur):
    #         self._create_table(dbCur)
    #
    #     # Filter each row to only hold approved keys using dictionary
    #     # comprehension and, of course, a common set of keys ;-)
    #     fldNames = self._flds.keys()
    #     for row in data:
    #         commonFlds = list(set(row.keys()) & set(fldNames))
    #
    #         if len(commonFlds) > 0:
    #             flds = ','.join(commonFlds)
    #             vals = ','.join("?" for (_) in commonFlds)
    #             # Using list comprehension to only pull values
    #             # that we want/need from a row of data
    #             dbCur.execute("INSERT INTO {}({}) VALUES({})".format(self._table, flds, vals),
    #                           [row[key] for key in commonFlds])
    #
    #     dbConn.commit()
    #     dbConn.close()

    # def get_data(self, numRecs=1, attribs=None):
    #     """Retrieve data records from SQLite database.
    #
    #     Args:
    #         numRecs:  Number of records to retrieve
    #         attribs:
    #             'orderBy'   - Field to sorted by
    #             'first'     - If TRUE, retrieve first 'numRecs' records.
    #                           Else retrieve last 'numRecs' records.
    #     Returns:
    #         list:     List of all records retrieved
    #
    #     Raises:
    #         OSError: If unable to access or read data from SQLite database.
    #     """
    #     orderBy = self._parse_attribs(attribs, 'orderBy', None)
    #     first = self._parse_attribs(attribs, 'first', True)
    #
    #     dbConn = self._connect_server()
    #     dbCur = dbConn.cursor()
    #
    #     fldNames = self._flds.keys()
    #     flds = ','.join("{!s}".format(key) for key in fldNames)
    #     sortFld = list(fldNames)[0] if orderBy is None else orderBy
    #
    #     try:
    #         if first:
    #             dbCur.execute('SELECT {flds} FROM {tbl} {order} LIMIT {limit}'.format(
    #                 flds=flds,
    #                 tbl=self._table,
    #                 order=self._create_orderby_param(sortFld),
    #                 limit=numRecs
    #             ))
    #         else:
    #             dbCur.execute('SELECT * FROM (SELECT {flds} FROM {tbl} {inner} LIMIT {limit}) {order}'.format(
    #                 flds=flds,
    #                 tbl=self._table,
    #                 inner=self._create_orderby_param(sortFld, True),
    #                 limit=numRecs,
    #                 order=self._create_orderby_param(sortFld)
    #             ))
    #
    #         dataRecords = dbCur.fetchall()
    #
    #     except sqlite3.Error as e:
    #         raise OSError("Failed to retrieve data from SQLite database '{}'\n{}!".format(self._name, e))
    #
    #     dbConn.close()
    #
    #     data = []
    #     for row in dataRecords:
    #         # Create dictionary with keys from field name
    #         # list, mapped against values from database.
    #         data.append(dict(zip(self._flds.keys(), row)))
    #
    #     return data


# =========================================================
#              U T I L I T Y   F U N C T I O N S
# =========================================================
def create_orderby_param(inStr: str, flip: bool = False) -> str:
    """Helper: flip 'ordferBy' parameter.

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


def verify_table(
        dbName: Union[Path, str],
        dbTable: str,
        dataFields: Dict[str, str],
        formatMap: Dict[str, str],
        strict: bool,
) -> str:
    """Verify that database table exists.

    Create table if missing and 'strict' is 'False'.

    Args:
        dbName:
            Name of database (i.e. either a filename, ':memory:', etc.)
        dbTable:
            Name of database table
        dataFields:
            Data fields
        formatMap:
            Data field format map
        strict:
            If 'True' then exception is raised when file does not exist

    Returns:
        Name of 'dbTable'

    Raises:
        InvalidAttributeError: If database table does not exist and 'strict' mode
    """
    dbConn = connect_to_db(dbName)
    dbCur = dbConn.cursor()

    if not exist_table(dbCur, dbTable):
        print(f"{dbTable =} does NOT exist")
        print(f"{strict =}")
        if strict:
            log.error(f"Database table '{dbTable}' does not exist in '{dbName}'.")
            raise InvalidAttributeError(f"Database table '{dbTable}' does not exist in '{dbName}'.")

        create_table(dbCur, dbTable, dataFields, formatMap)

    dbConn.commit()
    dbConn.close()

    return dbTable
