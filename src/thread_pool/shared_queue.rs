use crossbeam::channel::{self, Receiver, Sender};
use std::thread;

pub struct SharedQueueThreadPool {
    sender: Sender<Message>,
    handles: Vec<thread::JoinHandle<()>>,
}

impl Drop for SharedQueueThreadPool {
    fn drop(&mut self) {
        let handles = std::mem::take(&mut self.handles);

        for _ in 0..handles.len() {
            _ = self.sender.send(Message::Terminate);
        }

        for handle in handles {
            handle.join().unwrap();
        }
    }
}

enum Message {
    Job(Box<dyn FnOnce() + Send + 'static>),
    Terminate,
}

impl super::ThreadPool for SharedQueueThreadPool {
    fn new(threads: u32) -> crate::Result<Self> {
        let (sender, receiver) = channel::unbounded();
        let mut handles = vec![];

        for _ in 0..threads {
            let recv_handle = receiver.clone();
            let handle = thread::spawn(move || run_worker(recv_handle));
            handles.push(handle);
        }

        Ok(Self { sender, handles })
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.sender.send(Message::Job(Box::new(job))).unwrap();
    }
}

fn run_worker(receiver: Receiver<Message>) {
    match receiver.recv().unwrap() {
        Message::Job(job) => match std::panic::catch_unwind(std::panic::AssertUnwindSafe(job)) {
            Ok(()) => run_worker(receiver),
            Err(_) => run_worker(receiver),
        },
        Message::Terminate => {}
    }
}
