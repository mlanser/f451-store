"""Demo for using f451 Datastore module."""
import argparse
import logging
import os
import re
import sys
from configparser import ConfigParser
from configparser import ExtendedInterpolation
from pathlib import Path
from typing import Any
from typing import Dict
from typing import List

import konsole
from faker import Faker
from rich import print as rprint
from rich import traceback
from rich.rule import Rule

import f451_store.constants as const
from . import __app_name__
from . import __version__
from f451_store.store import Store

# from f451_store.exceptions import MissingAttributeError


# =========================================================
#          G L O B A L    V A R S   &   I N I T S
# =========================================================
traceback.install()  # Ensure 'pretty' tracebacks
faker = Faker()  # Initialize 'Faker'

_APP_NAME_: str = "f451 Communications Module"
_APP_NORMALIZED_: str = re.sub(r"[^A-Z0-9]", "_", str(__app_name__).upper())
_APP_DIR_: Path = Path(__file__).parent

# OS ENVIRON variable names
_APP_ENV_CONFIG_: str = f"{_APP_NORMALIZED_}_CONFIG"
_APP_ENV_SECRETS_: str = f"{_APP_NORMALIZED_}_SECRETS"

# Default CONFIG, SECRETS, and LOG filenames
# NOTE: these default filenames are used to search for files in degfault
#       locations if no files are indicated in OS ENVIRON vars or supplied
#       in CLI args.
# NOTE: we allow user to store secrets (i.e. API keys and other environ
#       secrets that should not go github), separately from 'safe' config
#       values (i.e. info that is safe to share on github). But both types
#       can also be stored in the same file.
_APP_LOG_: str = "f451-store.log"
_APP_CONFIG_: str = "f451-store.config.ini"
_APP_SECRETS_: str = "f451-store.secrets.ini"


# =========================================================
#              H E L P E R   F U N C T I O N S
# =========================================================
def _pretty_print_storage_info(
    srvName: Any, dbName: Any, numRecs: Any, totRecs: Any
) -> None:
    rprint(f"{srvName} DB: {dbName}")
    rprint(f"Generated and stored {numRecs} new data records.")
    rprint(f"There are now a total of {totRecs} records stored.")
    rprint(Rule())


def _create_sample_data(recs: int = 0) -> List[Dict[str, Any]]:
    data = []
    recs = max(0, min(recs, 100))

    for _ in range(recs):
        data.append(
            {
                "ssn": faker.ssn(),
                "name": faker.name(),
                "addr": faker.street_address(),
                "salary": faker.pyint(1000, 9999) * 100,
            }
        )

    return data


def init_cli_parser() -> argparse.ArgumentParser:
    """Initialize CLI (ArgParse) parser.

    Initialize the ArgParse parser with the CLI 'arguments' and
    return a new parser instance.

    Returns:
        ArgParse parser instance
    """
    parser = argparse.ArgumentParser(
        prog=__app_name__,
        description=f"Store data via 'f451 Datastore' [v{__version__}] module",
        epilog="NOTE: Only call a module if the corresponding data storage is installed",
    )

    parser.add_argument(
        "-V",
        "--version",
        action="store_true",
        help="Display module version number and exit.",
    )
    parser.add_argument("-d", "--debug", action="store_true", help="Run in debug mode")
    parser.add_argument(
        "--store",
        action="store",
        # default=const.STORAGE_SQLITE,
        # default=const.STORAGE_JSON,
        # default=const.STORAGE_CSV,
        default=const.STORAGE_ALL,
        type=str,
        help="Data storage file, database, or service to use",
    )
    parser.add_argument(
        "--type",
        action="store",
        type=str,
        help="Data storage type to use (e.g. CSV, JSON, SQL, etc.)",
    )

    parser.add_argument(
        "--recs", action="store", default=10, type=int, help="Number of recs to store"
    )

    parser.add_argument(
        "--action",
        action="store",
        default=const.KWD_STORE,
        type=str,
        help="Action: 'store' or 'retrieve'",
    )
    parser.add_argument(
        "--secrets",
        action="store",
        type=str,
        help="Path to primary config file",
    )
    parser.add_argument(
        "--config",
        action="store",
        type=str,
        help="Path to secondary config file",
    )
    parser.add_argument(
        "--log",
        action="store",
        type=str,
        help="Path to log file",
    )

    return parser


def init_ini_parser(fNames: Any) -> ConfigParser:
    """Initialize ConfigParser.

    Args:
        fNames:
            list with one or more paths to config files

    Returns:
        ConfigParser instance

    Raises:
        ValueError: Config file does not exist
    """
    parser = ConfigParser(interpolation=ExtendedInterpolation())

    tmpList = fNames if isinstance(fNames, list) else [fNames]
    for fn in tmpList:
        tmpName = Path(fn).expanduser()
        if not tmpName.exists():
            raise ValueError(f"Config file '{tmpName}' does not exist.")

        parser.read(tmpName)

    return parser


def get_valid_location(inFName: str) -> str:
    """Get valid location for a given filename.

    We use this to look for a given file (mainly config
    files) in a few default locations.

    Args:
        inFName:
            filename (string) to look for

    Returns:
        filename as string
    """
    cleanFName = inFName.strip("/")
    defaultLocations = [
        f"{Path.cwd()}/{cleanFName}",
        f"{Path(__file__).parent.absolute()}/{cleanFName}",
        f"{Path.home()}/{cleanFName}",
        f"/etc/{__app_name__}/{cleanFName}",
    ]

    outFName = ""
    for item in defaultLocations:
        if Path(item).exists():
            outFName = str(item)
            break

    return outFName


# =========================================================
#      M A I N   F U N C T I O N    /   A C T I O N S
# =========================================================
def main(inArgs: Any = None) -> None:  # noqa: C901
    """Core function to run through demo.    # noqa: D417,D415

    This function will run through one or more 'demo' scenarios
    depending on the arguments passed to CLI.

    Note:
        - Application will exit with error level 1 if invalid communications
          channels are included

        - Application will exit with error level 0 if either no arguments are
          entered via CLI, or if arguments '-V' or '--version' are used. No message
          will be sent in that case.

    Args:
        inArgs:
            CLI arguments used to start application
    """
    cli = init_cli_parser()

    # Show 'help' and exit if no args
    cliArgs, unknown = cli.parse_known_args(inArgs)
    if (not inArgs and len(sys.argv) == 1) or (len(sys.argv) == 2 and cliArgs.debug):
        cli.print_help(sys.stdout)
        sys.exit(0)

    if cliArgs.version:
        rprint(f"{_APP_NAME_} ({__app_name__}) v{__version__}")
        sys.exit(0)

    # Initialize loggers
    logger = logging.getLogger()
    logging.basicConfig(
        filename=cliArgs.log or f"{_APP_DIR_}/{_APP_LOG_}",
        # encoding="utf-8",     # Not available in Python v3.8
        level=logging.INFO,
    )
    logger.setLevel(logging.DEBUG if cliArgs.debug else logging.INFO)

    konsole.config(level=konsole.DEBUG if cliArgs.debug else konsole.ERROR)

    # Initialize main Datastorage Module with 'config' and 'secrets' data
    store = Store(
        init_ini_parser(
            [
                (
                    cliArgs.config
                    or (
                        os.environ.get(_APP_ENV_CONFIG_)
                        or get_valid_location(_APP_CONFIG_)
                    )
                ),
                (
                    cliArgs.secrets
                    or (
                        os.environ.get(_APP_ENV_SECRETS_)
                        or get_valid_location(_APP_SECRETS_)
                    )
                ),
            ]
        )
    )

    # Exit if invalid storage service
    availableStorage = (
        store.valid_storage
        if cliArgs.store == const.STORAGE_ALL
        else store.process_storage_list(cliArgs.store.split(const.DELIM_STD))
    )

    if not store.is_valid_storage(availableStorage):
        rprint(f"ERROR: '{cliArgs.store}' is not a valid datastore!")
        sys.exit(1)

    data = _create_sample_data(cliArgs.recs)

    # -----------------------
    # Run communication demos
    # -----------------------
    rprint(Rule())
    # -----------------------
    rprint("[bold black on white] - Available Storage - [/bold black on white]")
    if store.storage:
        for key, val in store.storage.items():
            rprint(f"{key:.<20.20}: {'ON' if val else 'OFF'}")
    else:
        rprint("There are no storage services enabled!")
    rprint(Rule())

    # -----------------------
    # - 1 - Broadcast message based on args
    # rprint(
    #     f"[bold black on white] - Broadcast to {availableChannels} - [/bold black on white]"
    # )
    # try:
    #     store.send_message(cliArgs.msg, **{const.KWD_CHANNELS: availableChannels})
    #
    # except (MissingAttributeError, CommunicationsError) as e:
    #     rprint(e)

    # -----------------------
    # - 2 - Show CSV properties
    if const.STORAGE_CSV in availableStorage:
        store.storage[const.STORAGE_CSV].store_records(data)
        _pretty_print_storage_info(
            store.storage[const.STORAGE_CSV].serviceName,
            store.storage[const.STORAGE_CSV].db,
            cliArgs.recs,
            store.storage[const.STORAGE_CSV].totalRecords,
        )

    # -----------------------
    # - 3 - Show JSON properties
    if const.STORAGE_JSON in availableStorage:
        store.storage[const.STORAGE_JSON].store_records(data)
        _pretty_print_storage_info(
            store.storage[const.STORAGE_JSON].serviceName,
            store.storage[const.STORAGE_JSON].db,
            cliArgs.recs,
            store.storage[const.STORAGE_JSON].totalRecords,
        )

    # -----------------------
    # - 4 - Send SMS via Twilio
    # if const.CHANNEL_TWILIO in availableChannels:
    #     send_test_msg_via_twilio(store, cliArgs.msg)

    # -----------------------
    # - 5 - Send tweets and DMs via Twitter
    # if const.CHANNEL_TWITTER in availableChannels:
    #     send_test_msg_via_twitter(store, cliArgs.msg)

    # -----------------------
    rprint(Rule())


# =========================================================
#            G L O B A L   C A T C H - A L L
# =========================================================
# if __name__ == "__main__":
#     main()  # pragma: no cover
