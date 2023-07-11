use rayon::ThreadPool;

pub struct RayonThreadPool(ThreadPool);

impl super::ThreadPool for RayonThreadPool {
    fn new(threads: u32) -> crate::Result<Self> {
        let rayon = rayon::ThreadPoolBuilder::new()
            .num_threads(threads as usize)
            .build()
            .unwrap();
        Ok(RayonThreadPool(rayon))
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.0.install(job)
    }
}
