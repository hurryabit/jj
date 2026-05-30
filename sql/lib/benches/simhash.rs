use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::Throughput;
use criterion::criterion_group;
use criterion::criterion_main;
use jj_sql_lib::SimHasher;

fn bench_simhasher(c: &mut Criterion) {
    let mut group = c.benchmark_group("SimHasher");

    for size in [1024usize, 16 * 1024, 256 * 1024, 1024 * 1024] {
        let data: Vec<u8> = (0..size).map(|i| i as u8).collect();
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &data, |b, data| {
            b.iter(|| {
                let mut hasher = SimHasher::<8>::new();
                hasher.update(data);
                hasher.finish()
            });
        });
    }

    group.finish();
}

criterion_group!(benches, bench_simhasher);
criterion_main!(benches);
