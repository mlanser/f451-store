"""Test cases for 'utils' module."""
from configparser import ConfigParser

import pytest

import f451_store.utils as utils


# =========================================================
#     G L O B A L S   &   P Y T E S T   F I X T U R E S
# =========================================================
_DEFAULT_TEST_STRING_ = "DEFAULT_TEST_STRING"

_TRUE_VALUES_ = ["True", "trUe", "t", 1, "1", True]
_FALSE_VALUES_ = ["False", "noTrue", "F", 0, "0", False]


@pytest.fixture()
def valid_service_map():
    """Return valid service map info."""
    return {
        "json": "f451_json",
        "csv": "f451_csv",
        "sql": "f451_sqlite",
    }


# =========================================================
#                T E S T   F U N C T I O N S
# =========================================================
def test_process_key_value_map(valid_service_map):
    """Test processing key-value maps."""
    expectedMap = valid_service_map
    mapString = "json:f451_json|csv:f451_csv|sql:f451_sqlite"
    mapList = ["json:f451_json", "csv:f451_csv", "sql:f451_sqlite"]

    # Happy path
    processedMap = utils.process_key_value_map(mapString)
    assert processedMap == expectedMap

    processedMap = utils.process_key_value_map(mapList)
    assert processedMap == expectedMap

    # Not so happy paths
    mapList = ["json:", ":", "", "csv:f451_csv", "sql:f451_sqlite"]
    processedMap = utils.process_key_value_map(mapList)
    assert processedMap == {"csv": "f451_csv", "sql": "f451_sqlite"}

    mapString = "json:f451_json:csv:f451_csv"
    processedMap = utils.process_key_value_map(mapString)
    assert processedMap == {"json": "f451_json"}

    mapString = "json:f451_json:csv f451_csv|abc|f451_abc|sql:f451_sqlite|:f451_sqlite"
    processedMap = utils.process_key_value_map(mapString)
    assert processedMap == {"json": "f451_json", "sql": "f451_sqlite"}


@pytest.mark.parametrize(
    "mapList", [[], [" ", ": ", ""], "json:", "json|f451_json", ":|"]
)
def test_process_key_value_map_always_return_empty(mapList):
    """Test processing empty key-value maps."""
    processedMap = utils.process_key_value_map(mapList)
    assert processedMap == {}


# @pytest.mark.parametrize("testData", _VALID_PHONE_STRINGS_)
# def test_is_valid_phone_is_true(testData):
#     """Test validating phone number strings."""
#     assert utils.is_valid_phone(testData)


# @pytest.mark.parametrize("testData", _INVALID_PHONE_STRINGS_)
# def test_is_valid_phone_is_false(testData):
#     """Test validating phone number strings."""
#     assert not utils.is_valid_phone(testData)


def test_convert_attrib_str_to_list():
    """Test converting attribute strings to lists of attributes."""
    val = utils.convert_attrib_str_to_list("apple|banana|orange", "|")
    assert set(val) == {"apple", "banana", "orange"}

    val = utils.convert_attrib_str_to_list("apple|", "|")
    assert set(val) == {"apple"}

    val = utils.convert_attrib_str_to_list("apple", "|")
    assert set(val) == {"apple"}

    val = utils.convert_attrib_str_to_list("1|2|3|4|5", "|")
    assert set(val) == {"1", "2", "3", "4", "5"}

    val = utils.convert_attrib_str_to_list("1|2|3|4|5", "|", int)
    assert set(val) == {1, 2, 3, 4, 5}


@pytest.mark.parametrize("testData", _TRUE_VALUES_)
def test_convert_str_to_bool_is_true(testData):
    """Test converting string values to boolean values."""
    assert utils.convert_str_to_bool(testData)


@pytest.mark.parametrize("testData", _FALSE_VALUES_)
def test_convert_str_to_bool_is_false(testData):
    """Test converting string values to boolean values."""
    assert not utils.convert_str_to_bool(testData)


def test_process_string_list():
    """Test processing lists of string."""
    val = utils.process_string_list(["apple", "banana", "orange"], "_p_", "_s_", "_j_")
    assert val == "_p_apple_s__j__p_banana_s__j__p_orange_s_"

    val = utils.process_string_list(["apple", "banana", "orange"], "", "", "")
    assert val == "applebananaorange"

    val = utils.process_string_list(["apple", "", "orange"], "_p_", "_s_", "_j_")
    assert val == "_p_apple_s__j__p_orange_s_"

    val = utils.process_string_list(["apple", "", ""], "_p_", "_s_", "_j_")
    assert val == "_p_apple_s_"

    val = utils.process_string_list(["", "", ""], "_p_", "_s_", "_j_")
    assert val == ""

    val = utils.process_string_list([], "_p_", "_s_", "_j_")
    assert val == ""

    val = utils.process_string_list("apple|banana|orange", "_p_", "_s_", "_j_")
    assert val == "_p_apple_s__j__p_banana_s__j__p_orange_s_"


def test_static_parse_attrib(valid_attribs_dict, default_test_key, default_test_val):
    """Test parsing attribute lists."""
    val = utils.parse_attribs(valid_attribs_dict, default_test_key)
    assert val == default_test_val

    val = utils.parse_attribs(valid_attribs_dict, "INVALID_KEY")
    assert val is None

    val = utils.parse_attribs(valid_attribs_dict, "INVALID_KEY", _DEFAULT_TEST_STRING_)
    assert val == _DEFAULT_TEST_STRING_

    val = utils.parse_attribs(
        "INVALID_STRUCTURE", default_test_key, _DEFAULT_TEST_STRING_
    )
    assert val == _DEFAULT_TEST_STRING_


def test_static_convert_str_to_dict(valid_config_string, valid_config_dict):
    """Test converting strings to `dict` structures."""
    val = utils.convert_config_str_to_dict(valid_config_string)
    assert val == valid_config_dict

    with pytest.raises(ValueError) as e:
        utils.convert_config_str_to_dict("NO:SECTION")
    assert e.type == ValueError
    assert "NO:SECTION" in e.value.args[0]

    with pytest.raises(ValueError) as e:
        utils.convert_config_str_to_dict("|FOO:BAR")
    assert e.type == ValueError
    assert "Section label" in e.value.args[0]  # TODO - create better check

    with pytest.raises(ValueError) as e:
        utils.convert_config_str_to_dict("SECTION|")
    assert e.type == ValueError
    assert "Section items" in e.value.args[0]  # TODO - create better check


def test_static_process_config(valid_config_string, valid_config_dict, valid_config):
    """Test processing config files/values."""
    val = utils.process_config(valid_config_string)
    assert isinstance(val, ConfigParser)

    val = utils.process_config(valid_config_dict)
    assert isinstance(val, ConfigParser)

    val = utils.process_config(valid_config)
    assert isinstance(val, ConfigParser)

    with pytest.raises(ValueError) as e:
        utils.process_config(["TEST", "INVALID", "TYPE"])
    assert e.type == ValueError
    assert "list" in e.value.args[0]


def test_static_parse_defaults(valid_config, valid_attribs_dict, default_test_section):
    """Test parsing default settings/values."""
    val = utils.parse_defaults(valid_config, [default_test_section])
    assert val == valid_attribs_dict
