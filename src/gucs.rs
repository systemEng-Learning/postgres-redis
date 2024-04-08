use pgrx::*;
use std::ffi::CStr;

pub static PGD_REDIS_URL: GucSetting<Option<&'static CStr>> =
    GucSetting::<Option<&'static CStr>>::new(None);

pub static PGD_REDIS_TABLE: GucSetting<Option<&'static CStr>> =
    GucSetting::<Option<&'static CStr>>::new(None);

pub static PGD_KEY_COLUMN: GucSetting<Option<&'static CStr>> =
    GucSetting::<Option<&'static CStr>>::new(None);

pub static PGD_VALUE_COLUMN: GucSetting<Option<&'static CStr>> =
    GucSetting::<Option<&'static CStr>>::new(None);

pub static PGD_BG_DELAY: GucSetting<i32> = GucSetting::<i32>::new(10);

// Initialize all the relevant postgresql conf parameters type and value
pub fn init() {
    GucRegistry::define_string_guc(
        "postgres_redis.redis_url",
        "Redis URL",
        "The url of the redis server to send the data to.",
        &PGD_REDIS_URL,
        GucContext::Userset,
        GucFlags::default(),
    );
    GucRegistry::define_string_guc(
        "postgres_redis.table",
        "Table name",
        "The table name to track for query execution.",
        &PGD_REDIS_TABLE,
        GucContext::Userset,
        GucFlags::default(),
    );
    GucRegistry::define_string_guc(
        "postgres_redis.key_column",
        "Table column name",
        "The column name whose value will be used as the key in the redis hash.",
        &PGD_KEY_COLUMN,
        GucContext::Userset,
        GucFlags::default(),
    );
    GucRegistry::define_string_guc(
        "postgres_redis.value_column",
        "Table column name",
        "The column name whose value will be used as the value in the redis hash.",
        &PGD_VALUE_COLUMN,
        GucContext::Userset,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        "postgres_redis.bg_delay",
        "Delay between rounds in seconds",
        "The amount of delay between execution of the background worker",
        &PGD_BG_DELAY,
        1,
        std::i32::MAX,
        GucContext::Userset,
        GucFlags::default(),
    );
}
