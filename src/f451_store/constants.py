"""Global constants for f451 Datastore module.

This module holds all global constants used within various components of
the f451 Datastore module. Most constants are used as keyword equivalents
for attributes in .ini files.
"""
# =========================================================
#              M I S C .   C O N S T A N T S
# =========================================================
DELIM_STD: str = "|"
DELIM_VAL: str = ":"

# =========================================================
#    K E Y W O R D S   F O R   C O N F I G   F I L E S
# =========================================================
STORAGE_ALL: str = "all"
STORAGE_MAIN: str = "f451_main"
STORAGE_SQLITE: str = "f451_sqlite"

KWD_DEBUG: str = "debug"  # Reserved
KWD_LOG_LEVEL: str = "log_level"
KWD_SUPPRESS_ERROR: str = "suppress_errors"
KWD_TO: str = "to"
KWD_TO_STORAGE: str = "to_storage"

LOG_CRITICAL: str = "CRITICAL"
LOG_DEBUG: str = "DEBUG"
LOG_ERROR: str = "ERROR"
LOG_INFO: str = "INFO"
LOG_NOTSET: str = "NOTSET"
LOG_OFF: str = "OFF"
LOG_WARNING: str = "WARNING"

LOG_LVL_OFF: int = -1
LOG_LVL_MIN: int = -1
LOG_LVL_MAX: int = 100

ATTR_REQUIRED: bool = True
ATTR_OPTIONAL: bool = False

SRV_TYPE_MAIN: str = "main"
SRV_TYPE_SQLITE: str = "sqlite"

STATUS_SUCCESS: str = "success"
STATUS_FAILURE: str = "failure"
