use criterion::{criterion_group, criterion_main, Criterion};
use kvs::thread_pool::{RayonThreadPool, SharedQueueThreadPool, ThreadPool};
use kvs::{KvStore, KvsClient, KvsEngine, KvsServer, SledEngine};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::{Arc, Barrier};
use tempfile::TempDir;

const CONCURRENT_CLIENTS: usize = 20;
const REQUESTS_PER_CLIENT: usize = 50;

pub trait KvsEngineOpen: Sized {
    fn open(path: impl AsRef<std::path::Path>) -> kvs::Result<Self>;
}
impl KvsEngineOpen for KvStore {
    fn open(path: impl AsRef<std::path::Path>) -> kvs::Result<Self> {
        KvStore::open(path.as_ref())
    }
}
impl KvsEngineOpen for SledEngine {
    fn open(path: impl AsRef<std::path::Path>) -> kvs::Result<Self> {
        SledEngine::open(path)
    }
}

fn bench_writes<E: KvsEngine + KvsEngineOpen, T: ThreadPool + 'static>(c: &mut Criterion) {
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

        let pool = T::new(num_threads as u32).unwrap();
        let store = E::open(path).unwrap();
        let (server, close_handle) = KvsServer::bind(socket_addr, store, pool).unwrap();
        let server_thread = std::thread::spawn(|| {
            server.run().unwrap();
        });

        let client_thread_pool = T::new(CONCURRENT_CLIENTS as u32).unwrap();

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

fn bench_reads<E: KvsEngine + KvsEngineOpen, T: ThreadPool + 'static>(c: &mut Criterion) {
    let cores = num_cpus::get();
    let inputs = (1..(2 * cores)).filter(|x| *x == 1 || x % 2 == 0);

    let mut group = c.benchmark_group("shared_queue_reads");
    let temp = TempDir::new().unwrap();
    let thread_pool = T::new(200).unwrap();
    let path = temp.path();
    let store = KvStore::open(path.clone()).unwrap();

    let ipv4_addr = Ipv4Addr::new(127, 0, 0, 1);
    let mut port = 4006;
    let server_addr = SocketAddr::new(IpAddr::V4(ipv4_addr), port);
    port += 1;

    let (server, handle) = KvsServer::bind(server_addr, store.clone(), thread_pool).unwrap();
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

        let store = store.clone();
        let thread_pool = T::new(num_threads as u32).unwrap();
        let (server, close_handle) = KvsServer::bind(socket_addr, store, thread_pool).unwrap();
        let server_thread = std::thread::spawn(|| {
            server.run().unwrap();
        });
        let client_thread_pool = T::new(CONCURRENT_CLIENTS as u32).unwrap();

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

fn shared_queue_kvstore_writes(c: &mut Criterion) {
    bench_writes::<KvStore, SharedQueueThreadPool>(c);
}
fn shared_queue_kvstore_reads(c: &mut Criterion) {
    bench_reads::<KvStore, SharedQueueThreadPool>(c);
}
fn shared_queue_sled_writes(c: &mut Criterion) {
    bench_writes::<SledEngine, SharedQueueThreadPool>(c);
}
fn shared_queue_sled_reads(c: &mut Criterion) {
    bench_reads::<SledEngine, SharedQueueThreadPool>(c);
}
fn rayon_kvstore_writes(c: &mut Criterion) {
    bench_writes::<KvStore, RayonThreadPool>(c);
}
fn rayon_kvstore_reads(c: &mut Criterion) {
    bench_reads::<KvStore, RayonThreadPool>(c);
}
fn rayon_sled_writes(c: &mut Criterion) {
    bench_reads::<SledEngine, RayonThreadPool>(c);
}
fn rayon_sled_reads(c: &mut Criterion) {
    bench_reads::<SledEngine, RayonThreadPool>(c);
}

criterion_group!(
    benches,
    shared_queue_kvstore_reads,
    shared_queue_kvstore_writes,
    shared_queue_sled_writes,
    shared_queue_sled_reads,
    rayon_kvstore_writes,
    rayon_kvstore_reads,
    rayon_sled_writes,
    rayon_sled_reads
);
criterion_main!(benches);
