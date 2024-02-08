use pgrx::{pg_guard, pg_shmem_init, warning, PGRXSharedMemory, PgLwLock, prelude::*, shmem::*};


#[derive(Copy, Clone)]
pub struct Info {
    pub content: [char; 255],
    pub length: u8,
}

unsafe impl PGRXSharedMemory for Info {}

pub static VEC: PgLwLock<heapless::Vec<Info, 400>> = PgLwLock::new();

pub fn vec_drain() -> Vec<Info> {
    let mut vec = VEC.exclusive();
    let r = vec.iter().copied().collect::<Vec<Info>>();
    vec.clear();
    r
}

pub fn vec_count() -> i32 {
    VEC.share().len() as i32
}

pub fn vec_push(item: Info) {
    VEC.exclusive().push(item).unwrap_or_else(|_| warning!("Vector is full, discarding update"));
}

#[pg_guard]
pub fn init_vec() {
    pg_shmem_init!(VEC);
}