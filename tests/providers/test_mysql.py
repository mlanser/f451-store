"""Test cases for MySQL module."""
import uuid
from inspect import currentframe  # noqa: F401
from inspect import getframeinfo  # noqa: F401
from pathlib import Path

import pytest
from faker import Faker

import f451_store.constants as const  # noqa: F401
import f451_store.providers.mysql as mysql
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
    return mysql.FORMAT_MAP


@pytest.fixture(scope="function")
def host_name(tmpdir_factory):
    """Create filename, but not actual SQLite data storage file."""
    return str(tmpdir_factory.mktemp("test").join(f"{uuid.uuid4().hex}.sqlite"))


@pytest.fixture(scope="function")
def valid_table_name():
    """Return valid table name."""
    return TEST_TABLE


# =========================================================
#                T E S T   F U N C T I O N S
# =========================================================
def test_create_mysql_db(
    file_based_storage, valid_table_name, valid_field_map, valid_format_map, capsys, helpers
):
    """Test happy path."""
    dbName = file_based_storage
    dbTable = valid_table_name
    mysqlDB = mysql.MySQL(valid_field_map, dbName, dbTable)

    assert str(mysql.db) == dbName
    assert mysql.dbTable == dbTable
    assert not len(set(mysqlDB.dataFields.keys()).difference(set(valid_field_map)))
    assert not len(set(mysqlDB.formatMap.keys()).difference(set(valid_format_map)))
