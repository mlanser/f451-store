"""PyTest fixtures and helper functions, etc."""
import pprint
import random
import uuid
from configparser import ConfigParser
from configparser import ExtendedInterpolation
from inspect import getframeinfo

import pytest
from faker import Faker

import f451_store.constants as const

# from pathlib import Path


# =========================================================
#                      H E L P E R S
# =========================================================
class Helpers:
    """Generic helper class.

    This class provides utility methods that can be accessed
    from within test fumnctions.
    """

    @staticmethod
    def pp(capsys, data, frame=None):
        """(Magic) Pretty Print function."""
        with capsys.disabled():
            _PP_ = pprint.PrettyPrinter(indent=4)
            print("\n")
            if frame is not None:
                print(f"LINE #: {getframeinfo(frame).lineno}\n")
            _PP_.pprint(data)


# =========================================================
#        G L O B A L   P Y T E S T   F I X T U R E S
# =========================================================
_KWD_TEST_SCTN_ = "test_sctn"  # NOTE: ConfigParser converts all keys to
_KWD_TEST_KEY_ = "test_key"  # lower case as they follow .ini syntax
_KWD_TEST_VAL_ = "test_val"  # rules for key attributes

KEY_FLD_NAME = 'ID'  # We're using this in various tests

_DEFAULT_ATTRIBS_DICT_ = {
    _KWD_TEST_KEY_: _KWD_TEST_VAL_,
    "k11": "v11",
    "k12": "v12",
}
_DEFAULT_CONFIG_DICT_ = {_KWD_TEST_SCTN_: _DEFAULT_ATTRIBS_DICT_}
_DEFAULT_CONFIG_STR_ = (
    f"{_KWD_TEST_SCTN_}|{_KWD_TEST_KEY_}:{_KWD_TEST_VAL_},k11:v11,k12:v12"
)
_DEFAULT_SERVICE_STR_ = "f451_json|f451_sqlite"

_DEFAULT_TEST_SECRETS_ = {
    "f451_json": {
        "fname": "_FNAME_OF_JSON_FILE_",
    },
    "f451_sqlite": {
        "fname": "_FNAME_OF_SQLITE_FILE_",
    },
}

_DEFAULT_TEST_CONFIG_ = {
    "f451_main": {
        "services": _DEFAULT_SERVICE_STR_,
        "service_map": "json:f451_json|csv:f451_csv|sql:f451_sqlite",  # noqa: B950
    },
    "f451_json": {},
    "f451_csv": {
        "delim": ",",
    },
    "f451_sqlite": {},
}

_TEST_FLD_MAP_ = {
    KEY_FLD_NAME: const.FMT_KWD_INTIDX,
    "HDR1": const.FMT_KWD_STR,
    "HDR2": const.FMT_KWD_STR,
    "HDR3": const.FMT_KWD_STR,
    "HDR4": const.FMT_KWD_STR,
}

_TEST_MIXED_FLD_MAP_ = {
    KEY_FLD_NAME: const.FMT_KWD_INTIDX,
    "HDR_STRIDX": const.FMT_KWD_STRIDX,
    "HDR_STR": const.FMT_KWD_STR,
    "HDR_INT": const.FMT_KWD_INT,
    "HDR_INTIDX": const.FMT_KWD_INTIDX,
    "HDR_FLOAT": const.FMT_KWD_FLOAT,
    "HDR_BOOL": const.FMT_KWD_BOOL,
}

_MAGIC_TEST_NUMBER_ = 1000

fake = Faker()  # Initialize 'Faker'


@pytest.fixture()
def key_fld_name():
    """Return key field name."""
    return KEY_FLD_NAME


@pytest.fixture()
def magic_test_number():
    """Return magic test number."""
    return _MAGIC_TEST_NUMBER_


@pytest.fixture()
def valid_field_map():
    """Return valid data field map."""
    return _TEST_FLD_MAP_


@pytest.fixture()
def invalid_field_map():
    """Return invalid data field map."""
    return {
        KEY_FLD_NAME: const.FMT_KWD_INTIDX,
        "HDR1": "__INVALID__",
        "HDR2": const.FMT_KWD_STR,
        "HDR3": const.FMT_KWD_STR,
        "HDR4": const.FMT_KWD_STR,
    }


@pytest.fixture(scope="function")
def valid_data_row():
    """Return valid data field map."""
    Faker.seed(0)
    return {
        KEY_FLD_NAME: fake.random_number(4, True),  # Random 4-digit numbers as index
        "HDR1": fake.word(),
        "HDR2": fake.word(),
        "HDR3": fake.word(),
        "HDR4": fake.word(),
    }


@pytest.fixture(scope="function")
def valid_data_set():
    """Return valid data set."""
    Faker.seed(0)
    numRecs = fake.random_number(3, True)
    return [
        {
            KEY_FLD_NAME: (i + 1),
            "HDR1": fake.word(),
            "HDR2": fake.word(),
            "HDR3": fake.word(),
            "HDR4": fake.word(),
        }
        for i in range(numRecs)
    ]


@pytest.fixture(scope="function")
def valid_mixed_field_map():
    """Return valid field map with mixed data types."""
    return _TEST_MIXED_FLD_MAP_


@pytest.fixture(scope="function")
def valid_mixed_data_set():
    """Return valid data set with mixed data types."""
    Faker.seed(0)
    numRecs = fake.random_number(3, True)
    return [
        {
            KEY_FLD_NAME: (i + 1),
            "HDR_STRIDX": str(i + fake.random_int(min=100, max=999)),
            "HDR_STR": fake.word(),
            "HDR_INT": fake.random_int(min=100, max=999),
            "HDR_INTIDX": (i + fake.random_int(min=100, max=999)),
            "HDR_FLOAT": round(random.random(), 2),  # noqa: S311
            "HDR_BOOL": bool(random.getrandbits(1)),
        }
        for i in range(numRecs)
    ]


@pytest.fixture()
def default_test_section():
    """Return default test values."""
    return _KWD_TEST_SCTN_


@pytest.fixture()
def default_test_key():
    """Return default test values."""
    return _KWD_TEST_KEY_


@pytest.fixture()
def default_test_val():
    """Return default test values."""
    return _KWD_TEST_VAL_


@pytest.fixture()
def valid_config():
    """Return valid config values."""
    parser = ConfigParser(interpolation=ExtendedInterpolation())
    parser.read_dict(_DEFAULT_CONFIG_DICT_)
    return parser


@pytest.fixture()
def valid_config_dict():
    """Return valid config values as `dict`."""
    return _DEFAULT_CONFIG_DICT_


@pytest.fixture()
def valid_config_string():
    """Return valid config values as `str`."""
    return _DEFAULT_CONFIG_STR_


@pytest.fixture()
def valid_attribs_dict():
    """Return attributes."""
    return _DEFAULT_ATTRIBS_DICT_


@pytest.fixture()
def new_config_file(tmpdir_factory):
    """Create actual config file."""
    configFile = tmpdir_factory.mktemp("test").join(f"{uuid.uuid4().hex}.ini")
    with open(configFile, "w") as fp:
        fp.write("[section]\nkey = value")

    return str(configFile)


@pytest.fixture()
def helpers():
    """Return `Helper` object.

    This makes it easier to use helper functions inside tests.
    """
    return Helpers


@pytest.fixture()
def invalid_file():
    """Create an invalid filename string."""
    return "/tmp/INVALID.FILE"  # noqa: S108


@pytest.fixture()
def invalid_string():
    """Create an invalid string."""
    return "INVALID_STRING"


@pytest.fixture()
def valid_settings():
    """Return valid config values."""
    parser = ConfigParser()
    parser.read_dict(_DEFAULT_TEST_CONFIG_)
    parser.read_dict(_DEFAULT_TEST_SECRETS_)
    return parser


@pytest.fixture()
def default_channels_string():
    """Return test values."""
    return _DEFAULT_SERVICE_STR_
