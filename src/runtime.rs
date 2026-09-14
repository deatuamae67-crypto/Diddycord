use std::{io, thread, time::Duration};

use tokio::runtime::{Builder, Runtime};

const MAX_WORKER_THREADS: usize = 4;
const WORKER_STACK_SIZE: usize = 1024 * 1024;

pub fn recommended_worker_threads() -> usize {
    thread::available_parallelism()
        .map(|count| count.get())
        .unwrap_or(1)
        .clamp(1, MAX_WORKER_THREADS)
}

pub fn build_runtime() -> io::Result<Runtime> {
    let workers = recommended_worker_threads();

    Builder::new_multi_thread()
        .worker_threads(workers)
        .max_blocking_threads(workers.max(2))
        .thread_keep_alive(Duration::from_secs(10))
        .thread_stack_size(WORKER_STACK_SIZE)
        .global_queue_interval(31)
        .event_interval(31)
        .enable_io()
        .enable_time()
        .build()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn worker_count_is_bounded() {
        let workers = recommended_worker_threads();
        assert!((1..=MAX_WORKER_THREADS).contains(&workers));
    }
}
