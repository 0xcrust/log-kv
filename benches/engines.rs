use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use kvs::{KvStore, KvsEngine, SledEngine};
use rand::{
    distributions::{Alphanumeric, DistString},
    thread_rng, Rng,
};
use tempfile::TempDir;

fn write(c: &mut Criterion) {
    let mut kv: Vec<(String, String)> = vec![];
    let dir = TempDir::new().unwrap();
    let dir = dir.path();

    let mut rng = thread_rng();
    for _ in 0..100 {
        let key_len: usize = rng.gen_range(1..100000);
        let val_len: usize = rng.gen_range(1..100000);
        kv.push((
            Alphanumeric.sample_string(&mut rng, key_len),
            Alphanumeric.sample_string(&mut rng, val_len),
        ));
    }

    let kvs = KvStore::open(&dir).unwrap();
    let mut kvs_group = c.benchmark_group("kvs write 100 values");
    for (id, (k, v)) in kv.iter().enumerate() {
        kvs_group.bench_with_input(BenchmarkId::from_parameter(id), &(k, v), |b, (k, v)| {
            b.iter(|| {
                kvs.set(k.to_string(), v.to_string()).unwrap();
            })
        });
    }
    kvs_group.finish();

    let sled = SledEngine::open(&dir).unwrap();
    let mut sled_group = c.benchmark_group("sled write 100 values");
    for (id, (k, v)) in kv.iter().enumerate() {
        sled_group.bench_with_input(BenchmarkId::from_parameter(id), &(k, v), |b, (k, v)| {
            b.iter(|| {
                sled.set(k.to_string(), v.to_string()).unwrap();
            })
        });
    }
    sled_group.finish();
}

fn read(c: &mut Criterion) {
    let mut kv: Vec<(String, String)> = vec![];
    let dir = TempDir::new().unwrap();
    let dir = dir.path();

    let kvs = KvStore::open(&dir).unwrap();
    let sled = SledEngine::open(&dir).unwrap();

    let mut rng = thread_rng();
    for _ in 0..1000 {
        let key_len: usize = rng.gen();
        let val_len: usize = rng.gen();
        kv.push((
            Alphanumeric.sample_string(&mut rng, key_len),
            Alphanumeric.sample_string(&mut rng, val_len),
        ));
    }

    for (k, v) in kv.clone() {
        kvs.set(k.clone(), v.clone()).expect("rb: kvs set failed");
        sled.set(k, v).expect("rb: sled set failed");
    }

    let mut kvs_group = c.benchmark_group("kvs read 1000 values");
    for (id, k) in kv.iter().map(|kv| kv.0.to_string()).enumerate() {
        kvs_group.bench_with_input(BenchmarkId::from_parameter(id), &k, |b, k| {
            b.iter(|| {
                kvs.get(k.to_string()).unwrap().unwrap();
            })
        });
    }
    kvs_group.finish();

    let mut sled_group = c.benchmark_group("sled read 1000 values");
    for (id, k) in kv.iter().map(|kv| kv.0.to_string()).enumerate() {
        sled_group.bench_with_input(BenchmarkId::from_parameter(id), &k, |b, k| {
            b.iter(|| {
                sled.get(k.to_string()).unwrap().unwrap();
            })
        });
    }
    sled_group.finish();
}

criterion_group!(benches, write, read);
criterion_main!(benches);
