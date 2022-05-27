"""f451 Datastore module.

This module acts as a common interface to several data storage modules and its
main purpose is to make it easy to store data using one or more storage mechanisms
at the same.

An application can store data in SQLite, in CSV or JSON files, or upload to cloud
storage using the ``store_data()`` method. It is also possible to store data in
specific systems using the ``store_data_in_<service>()`` methods.

This module assumes that data to be stored is provided in the form of
a ``list`` of ``dict`` structures and where at least one field should
be a unique index value (either ``int`` or ``str``) for the set.

For example:

    [
        {'ID': 'aa', 'fldName1': value, 'fldName2': value, ... , 'fldNameN': value},    # row 1
        {'ID': 'ab', 'fldName1': value, 'fldName2': value, ... , 'fldNameN': value},    # row 2
        {'ID': 'ac', 'fldName1': value, 'fldName2': value, ... , 'fldNameN': value},    # row 3
        ...
        {'ID': 'zz', 'fldName1': value, 'fldName2': value, ... , 'fldNameN': value},    # row N
    ]

    The list can be sorted by its index or any other field name using the ``sorted()`` method as follows:

        sortedlist = sorted(origList, key=itemgetter('<name of field to sort by>'))
"""
import logging
import pprint
from configparser import ConfigParser
from pathlib import Path
from typing import Any
from typing import Dict
from typing import List
from typing import Union

import f451_store.constants as const
import f451_store.providers.provider as provider
import f451_store.utils as utils
from f451_store.providers.csv import CSV
from f451_store.providers.json import JSON

# from f451_store.providers.sqlite import SQLite

# =========================================================
#          G L O B A L S   A N D   H E L P E R S
# =========================================================
_SRV_PROVIDER_: str = "Main"
_SRV_CONFIG_SCTN_: str = "f451_main"

log = logging.getLogger()
pp = pprint.PrettyPrinter(indent=4)

# typeDefProvider = Union[CSV, JSON, SQLIte, None]
typeDefProvider = Union[CSV, JSON, Any]
typeDefStringLists = Union[str, List[str], Any]
typeDefStorageInfo = Union[ConfigParser, Dict[str, str], Dict[str, Any], List[str], Any]
# typeDefSendMsgResponse = Union[List[provider.Response], Any]


# =========================================================
#        M A I N   C L A S S   D E F I N I T I O N
# =========================================================
class Store(provider.Provider):
    """Main class for f451 Datastore module.

    Use this main class as default interface to store data to and retrieve from
    any of the installed/enabled data storage services.

    All available services and associated attributes (e.g. API keys, etc.) are
    defined during initialization of the class.

    Attributes:
        config:
            set of attributes for 'secrets' such as API keys, general
            attributes, and default settings
    """

    def __init__(self, config: Any = None) -> None:
        super().__init__(const.SRV_TYPE_MAIN, _SRV_PROVIDER_, _SRV_CONFIG_SCTN_)

        settings = utils.process_config(config, False)

        self.default_storage = settings
        self.storage_map = settings
        self.storage = settings  # type: ignore[assignment]

    @property
    def storage(self) -> Dict[str, Any]:
        """Return 'storage' property."""
        return self._storage

    @storage.setter
    def storage(self, settings: ConfigParser) -> None:
        """Set 'storage' property."""
        self._storage = {
            const.STORAGE_CSV: self._init_csv(settings),
            const.STORAGE_JSON: self._init_json(settings),
            const.STORAGE_SQLITE: self._init_sqlite(settings),
        }

    @property
    def valid_storage(self) -> List[str]:
        """Return 'senderName' property."""
        return list(self._storage.keys())

    @property
    def storage_map(self) -> typeDefStorageInfo:
        """Return 'storage_map' property."""
        return self._storage_map

    @storage_map.setter
    def storage_map(self, settings: ConfigParser) -> None:
        """Set 'storage_map' property."""
        self._storage_map = utils.process_key_value_map(
            settings.get(const.STORAGE_MAIN, const.KWD_STORAGE_MAP, fallback="")
        )

    def is_valid_storage(self, inChannels: typeDefStringLists) -> bool:
        """Check if communications storage is valid."""
        tmpList = self._normalize_storage_list(inChannels)
        return (
            all(self._verify_storage(ch, True) for ch in tmpList) if tmpList else False
        )

    def is_enabled_storage(self, inChannels: typeDefStringLists) -> bool:
        """Check if communications storage is enabled."""
        tmpList = self._normalize_storage_list(inChannels)
        return (
            all(ch in self._storage and self._storage[ch] for ch in tmpList)
            if tmpList
            else False
        )

    @property
    def default_storage(self) -> typeDefStorageInfo:
        """Return 'default_storage' property."""
        return self._default_storage

    @default_storage.setter
    def default_storage(self, settings: ConfigParser) -> None:
        """Set 'default_storage' property."""
        self._default_storage = str(
            settings.get(const.STORAGE_MAIN, const.KWD_STORAGE, fallback="")
        ).split(const.DELIM_STD)

    def _verify_storage(self, chName: str, force: bool) -> bool:
        return (
            (chName != "" and (chName in self._storage or chName in self._storage_map))
            if force
            else (chName != "")
        )

    @staticmethod
    def _normalize_storage_list(inStorage: typeDefStringLists) -> List[str]:
        if inStorage:
            if isinstance(inStorage, str):
                return inStorage.split(const.DELIM_STD)
            elif all(isinstance(ch, str) for ch in inStorage):
                return inStorage

        return []

    @staticmethod
    def _init_csv(settings: ConfigParser) -> typeDefProvider:
        """Initialize 'CSV' storage provider."""
        if not settings.has_section(const.STORAGE_CSV):
            return None

        fldMap = utils.process_key_value_map(
            settings.get(const.STORAGE_MAIN, const.KWD_FIELD_MAP, fallback="")
        )

        return CSV(
            Path(settings.get(const.STORAGE_CSV, const.KWD_FNAME, fallback="")),
            fldMap,
        )

    @staticmethod
    def _init_json(settings: ConfigParser) -> typeDefProvider:
        """Initialize 'JSON' storage provider."""
        if not settings.has_section(const.STORAGE_JSON):
            return None

        fldMap = utils.process_key_value_map(
            settings.get(const.STORAGE_MAIN, const.KWD_FIELD_MAP, fallback="")
        )

        return JSON(
            Path(settings.get(const.STORAGE_JSON, const.KWD_FNAME, fallback="")),
            fldMap,
        )

    @staticmethod
    def _init_sqlite(settings: ConfigParser) -> typeDefProvider:
        """Initialize 'SQLite' storage provider."""
        return None

    def trim_records(self, numRecs: int = 0, **kwargs: Any) -> int:
        """Trim first or last 'numREcs' records."""
        return 0

    def count_records(self, **kwargs: Any) -> int:
        """Return number of records based on set of criteria."""
        return 0

    def store_records(
        self, inData: Union[List[Dict[str, Any]], Dict[str, Any]]
    ) -> None:
        """Stub for 'store_data()' method."""
        pass

    def retrieve_records(self, numRecs: int = 1, **kwargs: Any) -> List[Dict[str, Any]]:
        """Stub for 'retrieve_data()' method."""
        pass

    def process_storage_list(
        self, inList: typeDefStringLists, strict: bool = False
    ) -> List[str]:
        """Process list of storage service names.

        The purpose of this method is to process a list with one or more storage
        service names and placing them into a list.

        Args:
            inList:
                Single string or list with one or more strings
            strict:
                If 'True' then include only valid and enabled storage service names

        Returns:
            String with zero or more storage service names
        """
        tmpList = (
            inList
            if isinstance(inList, list)
            else utils.convert_attrib_str_to_list(inList)
        )

        print(inList)
        print(tmpList)

        outList = [
            ch
            for ch in [
                self._storage_map[item] if item in self._storage_map else item
                for item in [
                    tmp.strip()
                    for tmp in tmpList
                    if self._verify_storage(tmp.strip(), strict)
                ]
            ]
            if self.is_enabled_storage(ch)
        ]

        print(outList)
        return outList
