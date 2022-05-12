"""f451 Datastore module.

This module acts as a common interface to several data storage modules and its
main purpose is to make it easy to store data using one or more storage mechanisms
at the same.

An application can store data in SQLite, in CSV or JSON files, or upload to cloud
storage using the ``store_data()`` method. It is also possible to store data in
specific systems using the ``store_data_in_<service>()`` methods.
"""
import logging
import pprint
from typing import Any

import f451_store.constants as const
import f451_store.providers.provider as provider
import f451_store.utils as utils

# from configparser import ConfigParser

# from typing import Dict
# from typing import List
# from typing import Union

# from f451_store.exceptions import InvalidServiceError

# from f451_store.providers.csv import CSV
# from f451_store.providers.json import JSON
# from f451_store.providers.sqlite import SQLite

# =========================================================
#          G L O B A L S   A N D   H E L P E R S
# =========================================================
_SRV_PROVIDER_: str = "Main"
_SRV_CONFIG_SCTN_: str = "f451_main"

log = logging.getLogger()
pp = pprint.PrettyPrinter(indent=4)

# typeDefProvider = Union[Mailgun, Slack, Twilio, Twitter, None]
# typeDefStringLists = Union[str, List[str], None]
# typeDefChannelInfo = Union[
#     ConfigParser, Dict[str, str], Dict[str, Any], List[str], None
# ]
# typeDefSendMsgResponse = Union[List[provider.Response], Any]


# =========================================================
#        M A I N   C L A S S   D E F I N I T I O N
# =========================================================
class Store(provider.Provider):
    """Main class for f451 Communications module.

    Use this main class as default interface to send messages to any of the
    installed/enabled communications channels.

    All available channels and associated attributes (e.g. API keys, etc.) are
    defined during initialization of the class.

    Attributes:
        config:
            set of attributes for 'secrets' such as API keys, general
            attributes, and default settings
    """

    def __init__(self, config: Any = None) -> None:
        super().__init__(const.SRV_TYPE_MAIN, _SRV_PROVIDER_, _SRV_CONFIG_SCTN_)

        settings = utils.process_config(config, False)

        self.default_channels = settings
        self.channel_map = settings
        self.channels = settings

    # @property
    # def channels(self) -> typeDefChannelInfo:
    #     """Return 'channels' property."""
    #     return self._channels

    # @channels.setter
    # def channels(self, settings: ConfigParser) -> None:
    #     """Set 'channels' property."""
    #     self._channels = {
    #         const.CHANNEL_MAILGUN: self._init_mailgun(settings),
    #         const.CHANNEL_SLACK: self._init_slack(settings),
    #         const.CHANNEL_TWILIO: self._init_twilio(settings),
    #         const.CHANNEL_TWITTER: self._init_twitter(settings),
    #     }
