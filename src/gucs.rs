use pgrx::*;
use std::ffi::CStr;

pub static PGD_REDIS_URL: GucSetting<Option<&'static CStr>> =
    GucSetting::<Option<&'static CStr>>::new(None);

pub fn init() {
    GucRegistry::define_string_guc(
        "postgres_redis.redis_url",
        "Redis URL",
        "The url of the redis server to send the data to.",
        &PGD_REDIS_URL,
        GucContext::Userset,
        GucFlags::default(),
    );
}
