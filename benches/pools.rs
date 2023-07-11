use criterion::{criterion_group, criterion_main, Criterion};
use kvs::thread_pool::{SharedQueueThreadPool, ThreadPool};
use kvs::{KvStore, KvsClient, KvsServer};
use rand::{
    distributions::{Alphanumeric, DistString},
    prelude::*,
};
use std::sync::{Arc, Barrier};
use tempfile::TempDir;

const KEY_LEN: usize = 20;
const VALUE_LEN: usize = 20;

fn shared_queue_threadpool_writes(c: &mut Criterion) {
    println!("is it even starting");
    let server_address = "127.0.0.1:4006".parse::<std::net::SocketAddr>().unwrap();
    let cores = num_cpus::get();
    let inputs = (1..(2 * cores)).filter(|x| *x == 1 || x % 2 == 0);

    let mut rng = thread_rng();
    let common_value = Alphanumeric.sample_string(&mut rng, VALUE_LEN);
    let key_values = (0..1000)
        .map(|_| {
            (
                Alphanumeric.sample_string(&mut rng, KEY_LEN),
                common_value.clone(),
            )
        })
        .collect::<Vec<_>>();

    let mut group = c.benchmark_group("shared_queue_writes");

    let temp = TempDir::new().unwrap();
    let kvstore = KvStore::open(temp.path()).unwrap();
    println!("got here. is inputs wrong??");
    for num_threads in inputs {
        //let temp = TempDir::new().unwrap();
        //let kvstore = KvStore::open(temp.path()).unwrap();

        let pool = SharedQueueThreadPool::new(num_threads as u32).unwrap();
        let server = KvsServer::bind(server_address, kvstore.clone(), pool).unwrap();
        let shutdown_handle = server.run().unwrap();
        let client_thread_pool = SharedQueueThreadPool::new(1000).unwrap();

        let benchmark_id = format!("{num_threads} threads benchmark");
        group.bench_with_input(benchmark_id, &key_values, |b, key_values| {
            b.iter(|| {
                println!("benching writes");
                let barrier = Arc::new(Barrier::new(key_values.len()));
                for (key, value) in key_values {
                    let b = Arc::clone(&barrier);
                    let (key, value) = (key.to_owned(), value.to_owned());
                    client_thread_pool.spawn(move || {
                        let mut client = KvsClient::connect(server_address).unwrap();
                        let result = client.set(key, value);
                        assert!(result.is_ok());
                        b.wait();
                    });
                }
                barrier.wait();
            })
        });

        //shutdown_handle.shutdown();
    }
    group.finish();
}

fn shared_queue_threadpool_reads(c: &mut Criterion) {
    let server_address = "127.0.0.1:4007".parse::<std::net::SocketAddr>().unwrap();
    let cores = num_cpus::get();
    let inputs = (1..(2 * cores)).filter(|x| *x == 1 || x % 2 == 0);

    let mut rng = thread_rng();
    let common_value = Alphanumeric.sample_string(&mut rng, VALUE_LEN);
    let key_values = (0..1000)
        .map(|_| {
            (
                Alphanumeric.sample_string(&mut rng, KEY_LEN),
                common_value.clone(),
            )
        })
        .collect::<Vec<_>>();

    let mut group = c.benchmark_group("shared_queue_reads");
    let temp = TempDir::new().unwrap();
    let thread_pool = SharedQueueThreadPool::new(200).unwrap();
    let kvstore = KvStore::open(temp.path()).unwrap();
    let server = KvsServer::bind(server_address, kvstore.clone(), thread_pool).unwrap();
    let close_handle = server.run().unwrap();

    let mut handles = vec![];
    for (key, value) in key_values.clone() {
        handles.push(std::thread::spawn(move || {
            let mut client = KvsClient::connect(server_address).unwrap();
            client.set(key.to_owned(), value.to_owned()).unwrap();
        }));
    }
    for handle in handles {
        handle.join().unwrap();
    }
    close_handle.shutdown();

    for num_threads in inputs {
        //let temp = TempDir::new().unwrap();
        //let engine = KvStore::open(temp.path()).unwrap();
        let thread_pool = SharedQueueThreadPool::new(num_threads as u32).unwrap();
        let server = KvsServer::bind(server_address, kvstore.clone(), thread_pool).unwrap();
        let shutdown_handle = server.run().unwrap();

        let client_thread_pool = SharedQueueThreadPool::new(1000).unwrap();

        let benchmark_id = format!("{num_threads} threads benchmark");
        group.bench_with_input(benchmark_id, &key_values, |b, key_values| {
            b.iter(|| {
                println!("benching reads");
                let barrier = Arc::new(Barrier::new(key_values.len()));
                for (key, value) in key_values {
                    let b = Arc::clone(&barrier);
                    let (key, value) = (key.to_owned(), value.to_owned());

                    client_thread_pool.spawn(move || {
                        let mut client = KvsClient::connect(server_address).unwrap();
                        let result = client.get(key).unwrap().unwrap();
                        assert!(result == value);
                        b.wait();
                    });
                }
                barrier.wait();
            })
        });

        //shutdown_handle.shutdown();
    }
    group.finish();
}

criterion_group!(
    benches,
    shared_queue_threadpool_reads,
    shared_queue_threadpool_writes
);
criterion_main!(benches);
