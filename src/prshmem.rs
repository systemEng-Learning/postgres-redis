use pgrx::{pg_guard, pg_shmem_init, prelude::*, shmem::*, warning, PGRXSharedMemory, PgLwLock};

#[derive(Copy, Clone)]
pub struct Info {
    pub content: [char; 255],
    pub length: u8,
}

unsafe impl PGRXSharedMemory for Info {}

pub static REDIS_BUFFER: PgLwLock<heapless::Vec<Info, 400>> = PgLwLock::new();

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

#[pg_guard]
pub fn init_redis_buffer() {
    pg_shmem_init!(REDIS_BUFFER);
}
