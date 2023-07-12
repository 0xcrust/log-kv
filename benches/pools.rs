use criterion::{criterion_group, criterion_main, Criterion};
use kvs::thread_pool::{SharedQueueThreadPool, ThreadPool};
use kvs::{KvStore, KvsClient, KvsServer};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::{Arc, Barrier};
use tempfile::TempDir;

const CONCURRENT_CLIENTS: usize = 20;
const REQUESTS_PER_CLIENT: usize = 50;

fn shared_queue_threadpool_writes(c: &mut Criterion) {
    let cores = num_cpus::get();
    let inputs = (1..(2 * cores)).filter(|x| *x == 1 || x % 2 == 0);

    let mut group = c.benchmark_group("shared_queue_writes");

    let temp = TempDir::new().unwrap();
    let path = temp.path();

    let ipv4_addr = Ipv4Addr::new(127, 0, 0, 1);
    let mut port = 4006;

    for num_threads in inputs {
        let socket_addr = SocketAddr::new(IpAddr::V4(ipv4_addr), port);
        port += 1;

        let pool = SharedQueueThreadPool::new(num_threads as u32).unwrap();
        let store = KvStore::open(path).unwrap();
        let (server, close_handle) = KvsServer::bind(socket_addr, store, pool).unwrap();
        let server_thread = std::thread::spawn(|| {
            server.run().unwrap();
        });

        let client_thread_pool = SharedQueueThreadPool::new(CONCURRENT_CLIENTS as u32).unwrap();

        let benchmark_id = format!("{num_threads} threads benchmark");
        group.bench_function(benchmark_id, |b| {
            b.iter(|| {
                let barrier = Arc::new(Barrier::new(CONCURRENT_CLIENTS + 1));
                for i in 0..CONCURRENT_CLIENTS {
                    let b = Arc::clone(&barrier);
                    let start = i * REQUESTS_PER_CLIENT;
                    let end = start + REQUESTS_PER_CLIENT;
                    client_thread_pool.spawn(move || {
                        let mut client = KvsClient::connect(socket_addr).unwrap();
                        for i in start..end {
                            let key = format!("key{i:0>width$}", width = 5);
                            let result = client.set(key.clone(), "x".to_string());
                            assert!(result.is_ok());
                        }
                        client.shutdown().unwrap();
                        b.wait();
                    });
                }
                barrier.wait();
            })
        });

        close_handle.shutdown().unwrap();
        server_thread.join().unwrap();
    }
    group.finish();
}

fn shared_queue_threadpool_reads(c: &mut Criterion) {
    let cores = num_cpus::get();
    let inputs = (1..(2 * cores)).filter(|x| *x == 1 || x % 2 == 0);

    let mut group = c.benchmark_group("shared_queue_reads");
    let temp = TempDir::new().unwrap();
    let thread_pool: SharedQueueThreadPool = SharedQueueThreadPool::new(200).unwrap();
    let path = temp.path();
    let kvstore = KvStore::open(path.clone()).unwrap();

    let ipv4_addr = Ipv4Addr::new(127, 0, 0, 1);
    let mut port = 4006;
    let server_addr = SocketAddr::new(IpAddr::V4(ipv4_addr), port);
    port += 1;

    let (server, handle) = KvsServer::bind(server_addr, kvstore.clone(), thread_pool).unwrap();
    let server_thread = std::thread::spawn(|| {
        server.run().unwrap();
    });
    let mut handles = vec![];
    for i in 0..1000 {
        handles.push(std::thread::spawn(move || {
            let key = format!("key{i:0>width$}", width = 5);
            let mut client = KvsClient::connect(server_addr).unwrap();
            client.set(key, "x".to_string()).unwrap();
        }));
    }
    for handle in handles {
        handle.join().unwrap();
    }
    handle.shutdown().unwrap();
    server_thread.join().unwrap();

    for num_threads in inputs {
        let socket_addr = SocketAddr::new(IpAddr::V4(ipv4_addr), port);
        port += 1;

        let store = kvstore.clone();
        let thread_pool = SharedQueueThreadPool::new(num_threads as u32).unwrap();
        let (server, close_handle) = KvsServer::bind(socket_addr, store, thread_pool).unwrap();
        let server_thread = std::thread::spawn(|| {
            server.run().unwrap();
        });
        let client_thread_pool = SharedQueueThreadPool::new(CONCURRENT_CLIENTS as u32).unwrap();

        let benchmark_id = format!("{num_threads} threads benchmark");
        group.bench_function(benchmark_id, |b| {
            b.iter(|| {
                let barrier = Arc::new(Barrier::new(CONCURRENT_CLIENTS + 1));
                for i in 0..CONCURRENT_CLIENTS {
                    let b = Arc::clone(&barrier);
                    let start = i * REQUESTS_PER_CLIENT;
                    let end = start + REQUESTS_PER_CLIENT;

                    client_thread_pool.spawn(move || {
                        let mut client = KvsClient::connect(socket_addr).unwrap();
                        for i in start..end {
                            let key = format!("key{i:0>width$}", width = 5);
                            let result = client.set(key, "x".to_string());
                            assert!(result.is_ok())
                        }
                        client.shutdown().unwrap();
                        b.wait();
                    });
                }
                barrier.wait();
            })
        });

        close_handle.shutdown().unwrap();
        server_thread.join().unwrap();
    }
    group.finish();
}

criterion_group!(
    benches,
    shared_queue_threadpool_reads,
    shared_queue_threadpool_writes
);
criterion_main!(benches);
