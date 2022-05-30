"""Test cases for CSV module."""
import uuid
from inspect import currentframe  # noqa: F401
from inspect import getframeinfo  # noqa: F401
from pathlib import Path

import pytest
from faker import Faker

import f451_store.constants as const  # noqa: F401
import f451_store.providers.csv as csv
from f451_store.exceptions import InvalidAttributeError

# =========================================================
#     G L O B A L S   &   P Y T E S T   F I X T U R E S
# =========================================================
TEST_DATA = "Hello World!"
TEST_DATA_HEADERS = "ID,HDR1,HDR2,HDR3,HDR4"
TEST_DATA_VALUES = "1,FLD1,FLD2,FLD3,FLD4"

fake = Faker()  # Initialize 'Faker'


@pytest.fixture()
def valid_format_map():
    """Return valid data format map."""
    return csv.FORMAT_MAP


@pytest.fixture(scope="function")
def existing_CSV_storage(tmpdir_factory):
    """Create an actual CSV data storage file."""
    testFile = tmpdir_factory.mktemp("test").join(f"{uuid.uuid4().hex}.csv")
    with open(testFile, "w", newline="") as fp:
        fp.write(f"{TEST_DATA_HEADERS}\n")
        fp.write(f"{TEST_DATA_VALUES}\n")

    return str(testFile)


@pytest.fixture(scope="function")
def non_existing_CSV_storage(tmpdir_factory):
    """Create filename, but not actual CSV data storage file."""
    return str(tmpdir_factory.mktemp("test").join(f"{uuid.uuid4().hex}.csv"))


# =========================================================
#                T E S T   F U N C T I O N S
# =========================================================
def test_create_csv_data_file(
    existing_CSV_storage, valid_field_map, valid_format_map, capsys, helpers
):
    """Test happy path."""
    dbFName = existing_CSV_storage
    csvDB = csv.CSV(valid_field_map, db_host=dbFName)

    assert str(csvDB.dbHost) == dbFName
    assert not len(set(csvDB.dataFields.keys()).difference(set(valid_field_map)))
    assert not len(set(csvDB.formatMap.keys()).difference(set(valid_format_map)))


def test_create_csv_data_file_create_mode(
    non_existing_CSV_storage, valid_field_map, valid_format_map, capsys, helpers
):
    """Succeed creating CSV object with non-existent file using 'append' mode."""
    dbFName = non_existing_CSV_storage
    csvDB = csv.CSV(valid_field_map, db_host=dbFName, create=True)

    assert str(csvDB.dbHost) == dbFName
    assert csvDB.isCreateMode
    assert not len(set(csvDB.dataFields.keys()).difference(set(valid_field_map)))
    assert not len(set(csvDB.formatMap.keys()).difference(set(valid_format_map)))


def test_create_csv_data_file_fail_w_invalid_filename(
    non_existing_CSV_storage, valid_field_map, capsys, helpers
):
    """Fail creating CSV object with invalid/non-existent file."""
    dbFName = non_existing_CSV_storage
    with pytest.raises(InvalidAttributeError) as e:
        # 'create' is 'False' by default, but we're setting it
        # explicitly here to better illustrate test params
        csv.CSV(valid_field_map, db_host=dbFName, create=False)

    assert e.type == InvalidAttributeError
    assert dbFName in e.value.args[0]


def test_create_csv_data_file_fail_w_invalid_data_fields(
    existing_CSV_storage, invalid_field_map, capsys, helpers
):
    """Fail creating CSV object with invalid data fields."""
    dbFName = existing_CSV_storage
    with pytest.raises(InvalidAttributeError) as e:
        csv.CSV(invalid_field_map, db_host=dbFName, create=True, append=True)
    assert e.type == InvalidAttributeError
    assert "data fields" in e.value.args[0]


def test_store_records(
    non_existing_CSV_storage, valid_field_map, valid_data_row, capsys, helpers
):
    """Test storing data to existing file."""
    dbFName = non_existing_CSV_storage
    db = csv.CSV(valid_field_map, db_host=dbFName, create=True, append=True)

    data = [valid_data_row for _ in range(5)]
    db.store_records(data)

    verDB = Path(dbFName)
    with verDB.open("r", newline="") as verDBPtr:
        verData = verDBPtr.readlines()

    assert len(verData) == (len(data) + 1)
    assert verData[0].rstrip("\r\n") == ",".join(valid_field_map.keys())

    # Ensure that all vals from orig 1st data row are found in retrieved row
    vals = list(data[0].values())
    verVals = (verData[1].rstrip("\r\n")).split(",")
    for _, val in enumerate(vals):
        assert str(val) in verVals


def test_count_records(
    non_existing_CSV_storage, valid_field_map, valid_data_set, capsys, helpers
):
    """Test counting data records in existing file."""
    numRecs = len(valid_data_set)
    dbFName = non_existing_CSV_storage
    db = csv.CSV(valid_field_map, db_host=dbFName, create=True, append=True)

    db.store_records(valid_data_set)
    foundRecs = db.count_records()

    assert numRecs == foundRecs


def test_retrieve_records(
    non_existing_CSV_storage, valid_field_map, valid_data_set, capsys, helpers
):
    """Test retrieving data records from existing file."""
    dbFName = non_existing_CSV_storage
    db = csv.CSV(valid_field_map, db_host=dbFName, create=True, append=True)
    db.store_records(valid_data_set)
    maxRecs = len(valid_data_set)

    # Check last/newest record
    foundRecs = db.retrieve_records(1, newest=True)
    assert len(foundRecs) == 1
    assert foundRecs[0]["ID"] == maxRecs

    # Check first/oldest record
    foundRecs = db.retrieve_records(1, newest=False)
    assert len(foundRecs) == 1
    assert foundRecs[0]["ID"] == 1

    # Check 10th last/newest record
    foundRecs = db.retrieve_records(10, newest=True)
    assert len(foundRecs) == 10
    assert foundRecs[0]["ID"] == (maxRecs - 10 + 1)

    # Check 10 first/oldest record
    foundRecs = db.retrieve_records(10, newest=False)
    assert len(foundRecs) == 10
    assert foundRecs[9]["ID"] == 10

    # Retrieve beyond max rec
    foundRecs = db.retrieve_records(maxRecs + 10, newest=True)
    assert len(foundRecs) == maxRecs
    assert foundRecs[0]["ID"] == 1
    assert foundRecs[-1]["ID"] == maxRecs


def test_retrieve_records_with_mixed_data_set(
    non_existing_CSV_storage,
    valid_mixed_field_map,
    valid_mixed_data_set,
    key_fld_name,
    capsys,
    helpers,
):
    """Test retrieving data records from existing file."""
    dbFName = non_existing_CSV_storage
    db = csv.CSV(valid_mixed_field_map, db_host=dbFName, create=True, append=True)
    db.store_records(valid_mixed_data_set)
    maxRecs = len(valid_mixed_data_set)

    # Check last/newest record
    foundRecs = db.retrieve_records(1, newest=True)
    assert len(foundRecs) == 1
    assert foundRecs[0][key_fld_name] == maxRecs


def test_trim_records(
    non_existing_CSV_storage, valid_field_map, valid_data_set, capsys, helpers
):
    """Test removing data records from existing file.

    Strategy tro test trim function:
        1) Generate 100 records
        2) Remove first/oldest record
        3) Remove last/newest record
        4) Remove another 10 (oldest) records
        5) Remove another 10 (newest) records
    """
    dbFName = non_existing_CSV_storage
    db = csv.CSV(valid_field_map, db_host=dbFName, create=True, append=True)
    numRecs = len(valid_data_set)
    db.store_records(valid_data_set)

    # Trim first/oldest record
    db.trim_records(1, oldest=True)
    foundRecs = db.retrieve_records(1, newest=False)
    assert foundRecs[0]["ID"] == 2

    # Trim last/newest record
    db.trim_records(1, oldest=False)
    foundRecs = db.retrieve_records(1, newest=True)
    assert foundRecs[0]["ID"] == (numRecs - 1)

    # Trim 10 first/oldest records
    db.trim_records(10, oldest=True)
    foundRecs = db.retrieve_records(1, newest=False)
    assert foundRecs[0]["ID"] == 12

    # Trim 10 last/newest record
    db.trim_records(10, oldest=False)
    foundRecs = db.retrieve_records(1, newest=True)
    assert foundRecs[0]["ID"] == (numRecs - 1 - 10)

    # Confirm that we have removed 1+1+10+10 record from original 100
    assert db.count_records() == (numRecs - 1 - 1 - 10 - 10)


# from inspect import currentframe, getframeinfo
# helpers.pp(capsys, data, currentframe())
# captured = capsys.readouterr()
# helpers.pp(capsys, captured.out, currentframe())
