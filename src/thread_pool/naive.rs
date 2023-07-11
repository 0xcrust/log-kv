use std::cell::RefCell;

pub struct NaiveThreadPool {
    #[allow(dead_code)]
    threads: u32,
    handles: RefCell<Vec<std::thread::JoinHandle<()>>>,
}

impl super::ThreadPool for NaiveThreadPool {
    fn new(threads: u32) -> crate::Result<Self> {
        Ok(NaiveThreadPool {
            threads,
            handles: RefCell::new(vec![]),
        })
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        let handle = std::thread::spawn(|| {
            job();
        });
        self.handles.borrow_mut().push(handle);
    }
}
