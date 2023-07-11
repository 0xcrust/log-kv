mod naive;
mod rayon_wrapper;
mod shared_queue;

pub use naive::*;
pub use rayon_wrapper::*;
pub use shared_queue::*;

use crate::Result;

pub trait ThreadPool: Sized + Send {
    fn new(threads: u32) -> Result<Self>;
    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static;
}
