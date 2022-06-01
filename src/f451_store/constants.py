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
STORAGE_CSV: str = "f451_csv"
STORAGE_JSON: str = "f451_json"
STORAGE_SQLITE: str = "f451_sqlite"

KWD_APPEND: str = "append"
KWD_CLOSE: str = "close"
KWD_CREATE: str = "create"
KWD_DEBUG: str = "debug"  # Reserved
KWD_FIELD_MAP: str = "field_map"
KWD_FNAME: str = "fname"
KWD_OLDEST: str = "oldest"
KWD_NEWEST: str = "newest"
KWD_LOG_LEVEL: str = "log_level"
KWD_MODE: str = "mode"
KWD_FORCE: str = "force"
KWD_SUPPRESS_ERROR: str = "suppress_errors"
KWD_SAVE: str = "save"
KWD_STORE: str = "store"
KWD_STORAGE: str = "storage"
KWD_STORAGE_MAP: str = "storage_map"
KWD_STRICT: str = "strict"
KWD_GET: str = "get"
KWD_RETRIEVE: str = "retrieve"
KWD_ORDER_BY: str = "order_by"
KWD_IN_MEMORY: str = ":memory:"  # Reserved for use with SQLite
KWD_DB_HOST: str = "db_host"  # Used for all services
KWD_DB_PORT: str = "db_port"  # Used for MySQL, Postgres
KWD_DB_NAME: str = "db_name"  # Used for MySQL, Postgres
KWD_DB_TABLE: str = "db_table"  # Used for SQLite, MySQL, Postgres
KWD_DB_USER_NAME: str = "db_user"  # Used for MySQL, Postgres
KWD_DB_USER_PSWD: str = "db_pswd"  # Used for MySQL, Postgres

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
SRV_TYPE_CSV: str = "csv"
SRV_TYPE_JSON: str = "json"
SRV_TYPE_SQL: str = "sql"

STATUS_SUCCESS: str = "success"
STATUS_FAILURE: str = "failure"

FMT_KWD_STR: str = "str"
FMT_KWD_STRIDX: str = "strIDX"
FMT_KWD_INT: str = "int"
FMT_KWD_INTIDX: str = "intIDX"
FMT_KWD_FLOAT: str = "float"
FMT_KWD_BOOL: str = "bool"
