"""PyTest fixtures and helper functions, etc."""
import pprint
import uuid
from configparser import ConfigParser
from configparser import ExtendedInterpolation
from inspect import getframeinfo

import pytest

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
    """Only create the filename, but not the actual file."""
    configFile = tmpdir_factory.mktemp("test").join(f"{uuid.uuid4().hex}.ini")
    configFile.write("[section]\nkey = value")

    return str(configFile)


# @pytest.fixture(scope="session")
# def new_attachment_file(tmpdir_factory):
#     """Create an actual dummy file."""
#     testFile = tmpdir_factory.mktemp("test").join(f"{uuid.uuid4().hex}.txt")
#     testFile.write("THIS IS A TEST FILE")
#
#     return str(testFile)


# @pytest.fixture(scope="session")
# def new_media_file():
#     """Link to an actual test image file."""
#     testFile = Path("test/test-image-small.gif")
#
#     return str(testFile)


@pytest.fixture()
def helpers():
    """Return `Helper` object.

    This makes it easier to use helper functions inside tests.
    """
    return Helpers


# @pytest.fixture()
# def default_test_msg(prefix="", suffix="", sep=" "):
#     """Create a random test string."""
#     return sep.join([prefix, uuid.uuid4().hex, suffix])


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
