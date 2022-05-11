"""Test cases for the '__main__' module."""
import sys  # noqa: F401
from configparser import ConfigParser
from inspect import currentframe  # noqa: F401
from inspect import getframeinfo  # noqa: F401
from unittest import mock  # noqa: F401

import pytest
from faker import Faker  # noqa: F401

from f451_store import __app_name__
from f451_store import __main__
from f451_store import __version__


# =========================================================
#     G L O B A L S   &   P Y T E S T   F I X T U R E S
# =========================================================
_KWD_VERSION_SHORT_ = "-V"
_KWD_VERSION_LONG_ = "--version"
_KWD_CHANNEL_ = "--channel"
_KWD_CONFIG_ = "--config"
_KWD_SECRETS_ = "--secrets"


@pytest.fixture()
def valid_attribs():
    """Return valid test attribs."""
    return {"one": False, "two": "something"}


@pytest.fixture()
def valid_string():
    """Return valid test string."""
    return "VALID TEST STRING"


@pytest.fixture
def fake_filesystem(fs):  # pylint:disable=invalid-name
    """Init fake filesystem.

    Variable name 'fs' causes a pylint warning. Provide a longer name
    acceptable to pylint for use in tests.
    """
    yield fs


# =========================================================
#                T E S T   F U N C T I O N S
# =========================================================
@pytest.mark.parametrize("kwd", [_KWD_VERSION_SHORT_, _KWD_VERSION_LONG_])
def test_main_show_version(capsys, kwd):
    """Test display of app version."""
    with pytest.raises(SystemExit) as e:
        __main__.main([kwd])

    captured = capsys.readouterr()
    result = captured.out

    assert __app_name__ in result
    assert __version__ in result

    assert e.type == SystemExit
    assert e.value.code == 0


