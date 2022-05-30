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
def docker_test_host():
    """Define host info for MySQL test instance running in docker."""
    return {
        const.KWD_DB_HOST:     "127.0.0.1",
        const.KWD_DB_PORT:     "3306",
        const.KWD_DB_NAME:     "mysql_db",
        const.KWD_DB_USER_NAME: "user",
        const.KWD_DB_USER_PSWD: "user",
    }


@pytest.fixture(scope="function")
def valid_table_name():
    """Return valid table name."""
    return TEST_TABLE


# =========================================================
#                T E S T   F U N C T I O N S
# =========================================================
def test_connect_to_mysql(
    docker_test_host, valid_table_name, valid_field_map, valid_format_map, capsys, helpers
):
    """Test happy path."""
    dbHost = docker_test_host[const.KWD_DB_HOST]
    dbPort = docker_test_host[const.KWD_DB_PORT]
    dbName = docker_test_host[const.KWD_DB_NAME]
    dbUserName = docker_test_host[const.KWD_DB_USER_NAME]
    dbUserPswd = docker_test_host[const.KWD_DB_USER_PSWD]
    dbTable = valid_table_name
    mysqlDB = mysql.MySQL(
        valid_field_map,
        db_host=dbHost,
        db_port=dbPort,
        db_name=dbName,
        db_table=dbTable,
        db_user=dbUserName,
        db_pswd=dbUserPswd,
    )
    dbConn = mysqlDB.connect_open(True)

    assert mysqlDB.dbHost == dbHost
    assert mysqlDB.dbTable == dbTable
    assert not len(set(mysqlDB.dataFields.keys()).difference(set(valid_field_map)))
    assert not len(set(mysqlDB.formatMap.keys()).difference(set(valid_format_map)))
    assert mysqlDB.isConnectionOpen

    mysqlDB.connect_close(dbConn, True)
    assert not mysqlDB.isConnectionOpen


def test_connect_to_mysql_fail_w_missing_db_info(
    docker_test_host, valid_table_name, valid_field_map, valid_format_map, capsys, helpers
):
    """Fail creating SQLite object with missing 'dbHost'."""
    dbHost = docker_test_host[const.KWD_DB_HOST]
    dbPort = docker_test_host[const.KWD_DB_PORT]
    dbName = docker_test_host[const.KWD_DB_NAME]
    dbUserName = docker_test_host[const.KWD_DB_USER_NAME]
    dbUserPswd = docker_test_host[const.KWD_DB_USER_PSWD]
    dbTable = valid_table_name

    # Test missing 'dbHost'
    with pytest.raises(InvalidAttributeError) as e:
        mysql.MySQL(
            valid_field_map,
            db_port=dbPort,
            db_name=dbName,
            db_table=dbTable,
            db_user=dbUserName,
            db_pswd=dbUserPswd,
        )
    assert e.type == InvalidAttributeError
    assert const.KWD_DB_HOST in e.value.args[0]

    # Test missing 'dbTable'
    with pytest.raises(InvalidAttributeError) as e:
        mysql.MySQL(
            valid_field_map,
            db_host=dbHost,
            db_port=dbPort,
            db_name=dbName,
            db_user=dbUserName,
            db_pswd=dbUserPswd,
        )
    assert e.type == InvalidAttributeError
    assert const.KWD_DB_TABLE in e.value.args[0]

    # Test missing 'dbUserName'
    with pytest.raises(InvalidAttributeError) as e:
        mysql.MySQL(
            valid_field_map,
            db_host=dbHost,
            db_port=dbPort,
            db_name=dbName,
            db_table=dbTable,
            db_pswd=dbUserPswd,
        )
    assert e.type == InvalidAttributeError
    assert const.KWD_DB_USER_NAME in e.value.args[0]

    # Test missing 'dbUserPswd'
    with pytest.raises(InvalidAttributeError) as e:
        mysql.MySQL(
            valid_field_map,
            db_host=dbHost,
            db_port=dbPort,
            db_name=dbName,
            db_table=dbTable,
            db_user=dbUserName,
        )
    assert e.type == InvalidAttributeError
    assert const.KWD_DB_USER_PSWD in e.value.args[0]


def test_connect_to_mysql_fail_w_invalid_data_fields(
    docker_test_host, valid_table_name, invalid_field_map, capsys, helpers
):
    """Fail creating SQLite object with invalid data fields."""
    dbHost = docker_test_host[const.KWD_DB_HOST]
    dbPort = docker_test_host[const.KWD_DB_PORT]
    dbName = docker_test_host[const.KWD_DB_NAME]
    dbUserName = docker_test_host[const.KWD_DB_USER_NAME]
    dbUserPswd = docker_test_host[const.KWD_DB_USER_PSWD]
    dbTable = valid_table_name

    with pytest.raises(InvalidAttributeError) as e:
        mysql.MySQL(
            invalid_field_map,
            db_host=dbHost,
            db_port=dbPort,
            db_name=dbName,
            db_table=dbTable,
            db_user=dbUserName,
            db_pswd=dbUserPswd,
        )
    assert e.type == InvalidAttributeError
    assert "Invalid data formats in data fields" in e.value.args[0]
    # captured = capsys.readouterr()
    # helpers.pp(capsys, captured.out, currentframe())
    # helpers.pp(capsys, f"{mysqlDB.dbHost =}", currentframe())
    # helpers.pp(capsys, f"{mysqlDB.dbPort =}", currentframe())

# helpers.pp(capsys, foundRecs[0][keyFldName], currentframe())
# helpers.pp(capsys, foundRecs[-1][keyFldName], currentframe())
# helpers.pp(capsys, foundRecs, currentframe())

# from inspect import currentframe, getframeinfo
# helpers.pp(capsys, data, currentframe())
# captured = capsys.readouterr()
# helpers.pp(capsys, captured.out, currentframe())
