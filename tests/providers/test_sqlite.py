"""Test cases for SQLite module."""
import uuid
from inspect import currentframe  # noqa: F401
from inspect import getframeinfo  # noqa: F401

import pytest
from faker import Faker

import f451_store.constants as const  # noqa: F401
import f451_store.providers.sqlite as sqlite
from f451_store.exceptions import InvalidAttributeError

# =========================================================
#     G L O B A L S   &   P Y T E S T   F I X T U R E S
# =========================================================
TEST_DATA = "Hello World!"
TEST_DATA_HEADERS = "ID,HDR1,HDR2,HDR3,HDR4"
TEST_DATA_VALUES = "1,FLD1,FLD2,FLD3,FLD4"
TEST_TABLE = "test_table"

fake = Faker()  # Initialize 'Faker'


@pytest.fixture()
def valid_format_map():
    """Return valid data format map."""
    return sqlite.FORMAT_MAP


@pytest.fixture(scope="function")
def file_based_storage(tmpdir_factory):
    """Create filename, but not actual SQLite data storage file."""
    return str(tmpdir_factory.mktemp("test").join(f"{uuid.uuid4().hex}.sqlite"))


@pytest.fixture(scope="function")
def file_based_local_storage():
    """Create filename, but not actual SQLite data storage file."""
    return f"./_temp_{uuid.uuid4().hex}.sqlite"


@pytest.fixture(scope="function")
def in_memory_storage():
    """Use in-memory storage for most tests."""
    return ":memory:"


@pytest.fixture(scope="function")
def valid_table_name():
    """Return valid table name."""
    return TEST_TABLE


# =========================================================
#                T E S T   F U N C T I O N S
# =========================================================
def test_connect_to_sqlite_data_file(
    file_based_storage, valid_table_name, valid_field_map, valid_format_map, capsys, helpers
):
    """Test happy path.

    Note: We are NOT checking for a given table. We're only checking whether
          we can connect to the DB itself.
    """
    dbFName = file_based_storage
    dbTable = valid_table_name
    sqliteDB = sqlite.SQLite(valid_field_map, db_host=dbFName, db_table=dbTable)
    dbConn = sqliteDB.connect_open(True)

    assert str(sqliteDB.dbHost) == dbFName
    assert not len(set(sqliteDB.dataFields.keys()).difference(set(valid_field_map)))
    assert not len(set(sqliteDB.formatMap.keys()).difference(set(valid_format_map)))
    assert sqliteDB.isConnectionOpen

    sqliteDB.connect_close(dbConn, True)
    assert not sqliteDB.isConnectionOpen


def test_connect_to_sqlite_in_memory(
    in_memory_storage, valid_table_name, valid_field_map, valid_format_map, capsys, helpers
):
    """Test happy path.

    Note: We are NOT checking for a given table. We're only checking whether
          we can connect to the DB itself.
    """
    dbFName = in_memory_storage
    dbTable = valid_table_name
    sqliteDB = sqlite.SQLite(valid_field_map, db_host=dbFName, db_table=dbTable)
    dbConn = sqliteDB.connect_open(True)

    assert str(sqliteDB.dbHost) == dbFName
    assert not len(set(sqliteDB.dataFields.keys()).difference(set(valid_field_map)))
    assert not len(set(sqliteDB.formatMap.keys()).difference(set(valid_format_map)))
    assert dbConn is not None

    sqliteDB.connect_close(dbConn, True)
    assert not sqliteDB.isConnectionOpen


def test_connect_to_sqlite_w_create_set_to_false(
    file_based_storage, valid_table_name, valid_field_map, valid_format_map, capsys, helpers
):
    """Test happy path with create and append flags set to 'False'.

    Note: The 'append' and 'create' flags only affect tables and not
          the actual db file.
    """
    dbFName = file_based_storage
    dbTable = valid_table_name
    sqliteDB = sqlite.SQLite(valid_field_map, db_host=dbFName, db_table=dbTable, create=False, append=False)
    dbConn = sqliteDB.connect_open(True)

    assert not sqliteDB.isCreateMode
    assert not sqliteDB.isAppendMode
    assert dbConn is not None

    sqliteDB.connect_close(dbConn, True)
    assert not sqliteDB.isConnectionOpen


def test_connect_to_sqlite_fail_w_missing_dbhost(
    valid_table_name, valid_field_map, capsys, helpers
):
    """Fail creating SQLite object with missing 'dbHost'."""
    dbTable = valid_table_name
    with pytest.raises(InvalidAttributeError) as e:
        sqlite.SQLite(valid_field_map, db_table=dbTable)
    assert e.type == InvalidAttributeError
    assert const.KWD_DB_HOST in e.value.args[0]


def test_connect_to_sqlite_fail_w_missing_dbtable(
    file_based_storage, valid_field_map, capsys, helpers
):
    """Fail creating SQLite object with missing 'dbTable'."""
    dbFName = file_based_storage
    with pytest.raises(InvalidAttributeError) as e:
        sqlite.SQLite(valid_field_map, db_host=dbFName)
    assert e.type == InvalidAttributeError
    assert const.KWD_DB_TABLE in e.value.args[0]


def test_connect_to_sqlite_fail_w_invalid_data_fields(
    in_memory_storage, valid_table_name, invalid_field_map, capsys, helpers
):
    """Fail creating SQLite object with invalid data fields."""
    dbFName = in_memory_storage
    dbTable = valid_table_name
    with pytest.raises(InvalidAttributeError) as e:
        sqlite.SQLite(invalid_field_map, db_host=dbFName, db_table=dbTable)
    assert e.type == InvalidAttributeError
    assert "Invalid data formats in data fields" in e.value.args[0]


def test_create_table(
    in_memory_storage, file_based_local_storage, valid_table_name, valid_field_map, capsys, helpers
):
    """Test checking if table exists.

    Note: This test is using in-memory database, and we need
          to ensure that database connection remains open.
    """
    dbFName = in_memory_storage
    dbTable = valid_table_name
    sqliteDB = sqlite.SQLite(valid_field_map, db_host=dbFName, db_table=dbTable)
    dbConn = sqliteDB.connect_open(True)

    # Confirm that we have a connection
    assert sqliteDB.isConnectionOpen

    # Explicitly create a new tables
    assert not sqliteDB.has_table(valid_table_name, close=False)   # Default table should not exist yet in new db
    sqliteDB.create_table(valid_table_name, close=False)
    assert sqliteDB.has_table(valid_table_name, close=False)   # Table should exist now

    assert not sqliteDB.has_table('some_new_test_table', close=False)   # Table should not exist yet
    sqliteDB.create_table('some_new_test_table', close=False)
    assert sqliteDB.has_table('some_new_test_table', close=False)   # Table should exist now

    # Explicitly close open connection
    sqliteDB.connect_close(dbConn, True)
    assert not sqliteDB.isConnectionOpen


def test_create_new_table_in_single_call(
    in_memory_storage, file_based_local_storage, valid_table_name, valid_field_map, capsys, helpers
):
    """Test checking if table exists.

    Note: This test is using in-memory database, and we need
          to ensure that database connection remains open.
    """
    dbFName = in_memory_storage
    dbTable = valid_table_name
    sqliteDB = sqlite.SQLite(valid_field_map, db_host=dbFName, db_table=dbTable)
    dbConn = sqliteDB.connect_open(True)  # Explicitly open and keep open for duration of test

    sqliteDB.create_table(valid_table_name, close=False)
    assert sqliteDB.has_table(valid_table_name, close=False)   # Table should exist now

    # Explicitly close open connection
    sqliteDB.connect_close(dbConn, True)
    assert not sqliteDB.isConnectionOpen


def test_store_records(
    in_memory_storage, valid_table_name, valid_field_map, valid_data_row, capsys, helpers
):
    """Test storing data to existing file.

    Note: This test is using in-memory database, and we need
          to ensure that database connection remains open.
    """
    dbFName = in_memory_storage
    dbTable = valid_table_name
    sqliteDB = sqlite.SQLite(valid_field_map, db_host=dbFName, db_table=dbTable)
    dbConn = sqliteDB.connect_open(True)  # Explicitly open and keep open for duration of test

    numRecs = 25
    data = [valid_data_row for _ in range(numRecs)]
    sqliteDB.store_records(data, close=False)
    assert sqliteDB.has_table(valid_table_name, close=False)

    foundRecs = sqliteDB.count_records(close=False)
    assert foundRecs == numRecs

    # Explicitly close open connection
    sqliteDB.connect_close(dbConn, True)
    assert not sqliteDB.isConnectionOpen


def test_store_records_with_mixed_data_types(
    in_memory_storage,
    valid_table_name,
    valid_mixed_field_map,
    valid_mixed_data_set,
    capsys,
    helpers,
):
    """Test storing data with mixed data types to existing file.

    Note: This test is using in-memory database, and we need
          to ensure that database connection remains open.
    """
    dbFName = in_memory_storage
    dbTable = valid_table_name
    sqliteDB = sqlite.SQLite(valid_mixed_field_map, db_host=dbFName, db_table=dbTable)
    dbConn = sqliteDB.connect_open(True)  # Explicitly open and keep open for duration of test

    data = valid_mixed_data_set
    numRecs = len(data)
    sqliteDB.store_records(data, close=False)
    assert sqliteDB.has_table(valid_table_name, close=False)

    foundRecs = sqliteDB.count_records(close=False)
    assert foundRecs == numRecs

    # Explicitly close open connection
    sqliteDB.connect_close(dbConn, True)
    assert not sqliteDB.isConnectionOpen


def test_count_records(
    in_memory_storage, valid_table_name, valid_field_map, valid_data_row, capsys, helpers
):
    """Test counting data records in existing file.

    Note: This test is using in-memory database, and we need
          to ensure that database connection remains open.
    """
    dbFName = in_memory_storage
    dbTable = valid_table_name
    sqliteDB = sqlite.SQLite(valid_field_map, db_host=dbFName, db_table=dbTable)
    dbConn = sqliteDB.connect_open(True)  # Explicitly open and keep open for duration of test
    foundRecs = sqliteDB.count_records(close=False)
    assert foundRecs == 0   # There should be 0 records!

    numRecs = 25
    data = [valid_data_row for _ in range(numRecs)]
    sqliteDB.store_records(data, close=False)
    foundRecs = sqliteDB.count_records(close=False)
    assert foundRecs == numRecs

    # Explicitly close open connection
    sqliteDB.connect_close(dbConn, True)
    assert not sqliteDB.isConnectionOpen


def test_retrieve_records(
    in_memory_storage,
    valid_table_name,
    valid_mixed_field_map,
    valid_mixed_data_set,
    key_fld_name,
    capsys,
    helpers,
):
    """Test counting data records in existing file.

    Note: This test is using in-memory database, and we need
          to ensure that database connection remains open.
    """
    keyFldName = key_fld_name
    dbFName = in_memory_storage
    dbTable = valid_table_name
    sqliteDB = sqlite.SQLite(valid_mixed_field_map, db_host=dbFName, db_table=dbTable)
    dbConn = sqliteDB.connect_open(True)  # Explicitly open and keep open for duration of test

    data = valid_mixed_data_set
    makeNumRecs = len(data)
    getNumRecs = 5
    sqliteDB.store_records(data, close=False)

    # Grab 'newest' records & sort by 'keyFldName' which will
    # hold a value == to row/record number.
    foundRecs = sqliteDB.retrieve_records(getNumRecs, newest=True, order_by=keyFldName, close=False)
    assert len(foundRecs) == getNumRecs

    assert foundRecs[0][keyFldName] == makeNumRecs - getNumRecs + 1
    assert foundRecs[-1][keyFldName] == makeNumRecs

    # Grab 'oldest' records & sort by 'keyFldName' which will
    # hold a value == to row/record number.
    foundRecs = sqliteDB.retrieve_records(getNumRecs, newest=False, order_by=keyFldName, close=False)
    assert len(foundRecs) == getNumRecs

    assert foundRecs[0][keyFldName] == 1
    assert foundRecs[-1][keyFldName] == getNumRecs

    # Explicitly close open connection
    sqliteDB.connect_close(dbConn, True)
    assert not sqliteDB.isConnectionOpen


def test_trim_records(
    in_memory_storage,
    valid_table_name,
    valid_mixed_field_map,
    valid_mixed_data_set,
    key_fld_name,
    capsys,
    helpers,
):
    """Test removing data records from existing file.

    Strategy tro test trim function:
        1) Generate X records
        2) Remove first/oldest record
        3) Remove last/newest record
        4) Remove another 10 (oldest) records
        5) Remove another 10 (newest) records

    Note: This test is using in-memory database, and we need
          to ensure that database connection remains open.
    """
    keyFldName = key_fld_name
    dbFName = in_memory_storage
    dbTable = valid_table_name
    sqliteDB = sqlite.SQLite(valid_mixed_field_map, db_host=dbFName, db_table=dbTable)
    dbConn = sqliteDB.connect_open(True)  # Explicitly open and keep open for duration of test

    data = valid_mixed_data_set
    makeNumRecs = len(data)
    sqliteDB.store_records(data, close=False)

    # Trim first/oldest record
    trimRecs = sqliteDB.trim_records(1, newest=False, order_by=keyFldName, close=False)
    foundRecs = sqliteDB.retrieve_records(1, newest=False, order_by=keyFldName, close=False)
    totRecs = sqliteDB.count_records(close=False)
    assert trimRecs == 1
    assert foundRecs[0][keyFldName] == 2
    assert totRecs == makeNumRecs - 1

    # Trim last/newest record
    trimRecs = sqliteDB.trim_records(1, newest=True, order_by=keyFldName, close=False)
    foundRecs = sqliteDB.retrieve_records(1, newest=True, order_by=keyFldName, close=False)
    totRecs = sqliteDB.count_records(close=False)
    assert trimRecs == 1
    assert foundRecs[0][keyFldName] == makeNumRecs - 1
    assert totRecs == makeNumRecs - 1 - 1

    # Trim 10 first/oldest records
    trimRecs = sqliteDB.trim_records(10, newest=False, order_by=keyFldName, close=False)
    foundRecs = sqliteDB.retrieve_records(1, newest=False, order_by=keyFldName, close=False)
    totRecs = sqliteDB.count_records(close=False)
    assert trimRecs == 10
    assert foundRecs[0][keyFldName] == 12
    assert totRecs == makeNumRecs - 1 - 1 - 10

    # Trim 10 last/newest record
    trimRecs = sqliteDB.trim_records(10, newest=True, order_by=keyFldName, close=False)
    foundRecs = sqliteDB.retrieve_records(1, newest=True, order_by=keyFldName, close=False)
    totRecs = sqliteDB.count_records(close=False)
    assert trimRecs == 10
    assert foundRecs[0][keyFldName] == makeNumRecs - 1 - 10
    assert totRecs == makeNumRecs - 1 - 1 - 10 - 10

# helpers.pp(capsys, foundRecs[0][keyFldName], currentframe())
# helpers.pp(capsys, foundRecs[-1][keyFldName], currentframe())
# helpers.pp(capsys, foundRecs, currentframe())

# from inspect import currentframe, getframeinfo
# helpers.pp(capsys, data, currentframe())
# captured = capsys.readouterr()
# helpers.pp(capsys, captured.out, currentframe())
