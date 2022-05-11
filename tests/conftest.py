"""PyTest fixtures and helper functions, etc."""
import pprint
import uuid
from configparser import ConfigParser
from configparser import ExtendedInterpolation
from inspect import getframeinfo
from pathlib import Path

import pytest


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
