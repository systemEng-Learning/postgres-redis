use pgrx::{pg_guard, pg_shmem_init, prelude::*, shmem::*, warning, PGRXSharedMemory, PgLwLock};

/// A key value struct containing the redis key and value. The key and value strings are stored as array
/// because this objects of this struct will be stored on the stack and not the heap. This implies that
/// the size of this struct has to be known beforehand. The length of these strings is stored in the
/// different length members.
#[derive(Copy, Clone)]
pub struct Info {
    pub key: [char; 127],
    pub value: [char; 127],
    pub key_length: i8,
    pub value_length: i8,
}

impl Info {
    /// Create a new Info object that contains the `key_string` and
    /// `value_string` along with their lengths. This object will be
    /// returned from the function.
    pub fn new(key_string: &str, value_string: &str) -> Info {
        let mut key = [' '; 127];
        for (i, c) in key_string.chars().enumerate() {
            key[i] = c;
        }
        let mut value = [' '; 127];
        for (i, c) in value_string.chars().enumerate() {
            value[i] = c;
        }
        Info {
            key,
            key_length: key_string.len() as i8,
            value,
            value_length: value_string.len() as i8,
        }
    }
}

unsafe impl PGRXSharedMemory for Info {}

pub static REDIS_BUFFER: PgLwLock<heapless::Vec<Info, 400>> = PgLwLock::new();

/// Extract and remove all the items from the shared memory vector buffer
pub fn move_redis_data() -> Vec<Info> {
    let mut vec = REDIS_BUFFER.exclusive();
    let r = vec.iter().copied().collect::<Vec<Info>>();
    vec.clear();
    r
}

pub fn data_size() -> i32 {
    REDIS_BUFFER.share().len() as i32
}

pub fn add_item(item: Info) {
    REDIS_BUFFER
        .exclusive()
        .push(item)
        .unwrap_or_else(|_| warning!("Vector is full, discarding update"));
}

pub fn init_redis_buffer() {
    pg_shmem_init!(REDIS_BUFFER);
}
