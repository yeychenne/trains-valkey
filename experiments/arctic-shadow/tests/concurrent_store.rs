use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;

use trains_valkey::{Command, RedisStore, Reply};
use trains_valkey_arctic_shadow::{ConcurrentArcticStore, OrderedArcticStore};

fn command(argv: &[&[u8]]) -> Command {
    Command::parse(argv.iter().map(|arg| arg.to_vec()).collect()).expect("non-empty command")
}

#[test]
fn concurrent_arctic_store_handles_shared_reads_writes_and_removes() {
    const THREADS: usize = 8;
    const KEYS_PER_THREAD: usize = 2_000;

    let store = Arc::new(ConcurrentArcticStore::new());
    let written = Arc::new(Barrier::new(THREADS));
    let mut workers = Vec::new();

    for worker in 0..THREADS {
        let store = store.clone();
        let written = written.clone();
        workers.push(thread::spawn(move || {
            for index in 0..KEYS_PER_THREAD {
                let key = format!("concurrent:{worker}:{index:04}");
                let value = format!("value:{worker}:{index:04}");
                assert_eq!(store.set(key.as_bytes(), value.as_bytes()), Reply::ok());
            }
            let hot = format!("concurrent:hot:{}", worker % 4);
            assert_eq!(
                store.set(hot.as_bytes(), worker.to_string().as_bytes()),
                Reply::ok()
            );

            written.wait();

            for index in 0..KEYS_PER_THREAD {
                let key = format!("concurrent:{worker}:{index:04}");
                let expected = format!("value:{worker}:{index:04}").into_bytes();
                assert_eq!(store.get(key.as_bytes()), Reply::Bulk(expected));
                if index.is_multiple_of(2) {
                    assert!(store.remove(key.as_bytes()).unwrap());
                }
            }
        }));
    }

    for worker in workers {
        worker.join().expect("concurrent Arctic worker panicked");
    }

    let expected = THREADS * (KEYS_PER_THREAD / 2) + 4;
    assert_eq!(store.len(), expected);
    assert_eq!(store.snapshot_sorted().len(), expected);
    for hot in 0..4 {
        assert!(matches!(
            store.get(format!("concurrent:hot:{hot}").as_bytes()),
            Reply::Bulk(_)
        ));
    }
}

#[test]
fn ordered_writer_and_shared_readers_make_progress_together() {
    const READERS: usize = 8;
    const READS_PER_THREAD: usize = 20_000;

    let mut writer = OrderedArcticStore::new();
    for index in 0..64 {
        let key = format!("ordered:stable:{index:02}");
        assert_eq!(
            writer.apply(&command(&[b"SET", key.as_bytes(), b"stable"])),
            Reply::ok()
        );
    }
    assert_eq!(
        writer.apply(&command(&[b"SET", b"ordered:hot", b"0"])),
        Reply::ok()
    );

    let reader = writer.reader();
    let start = Arc::new(Barrier::new(READERS + 1));
    let mut workers = Vec::new();
    for worker in 0..READERS {
        let reader = reader.clone();
        let start = Arc::clone(&start);
        workers.push(thread::spawn(move || {
            start.wait();
            for index in 0..READS_PER_THREAD {
                let stable = format!("ordered:stable:{:02}", (index + worker) % 64);
                assert_eq!(
                    reader.query(&command(&[b"GET", stable.as_bytes()])),
                    Reply::Bulk(b"stable".to_vec())
                );
                assert!(matches!(
                    reader.query(&command(&[b"GET", b"ordered:hot"])),
                    Reply::Bulk(_)
                ));
            }
        }));
    }

    start.wait();
    for value in 1..=10_000 {
        let value = value.to_string();
        assert_eq!(
            writer.apply(&command(&[b"SET", b"ordered:hot", value.as_bytes()])),
            Reply::ok()
        );
    }
    for worker in workers {
        worker.join().expect("shared Arctic reader panicked");
    }

    assert_eq!(
        reader.query(&command(&[b"GET", b"ordered:hot"])),
        Reply::Bulk(b"10000".to_vec())
    );
    assert!(matches!(
        reader.query(&command(&[b"DBSIZE"])),
        Reply::Error(_)
    ));
    assert!(matches!(
        reader.query(&command(&[
            b"EXISTS",
            b"ordered:stable:00",
            b"ordered:stable:01"
        ])),
        Reply::Error(_)
    ));
    assert_eq!(writer.query(&command(&[b"DBSIZE"])), Reply::Integer(65));
    assert_eq!(
        writer.query(&command(&[
            b"EXISTS",
            b"ordered:stable:00",
            b"ordered:stable:01"
        ])),
        Reply::Integer(2)
    );
}

#[test]
fn snapshot_install_atomically_replaces_the_reader_generation() {
    const READERS: usize = 4;

    let mut writer = OrderedArcticStore::new();
    assert_eq!(
        writer.apply(&command(&[b"SET", b"old:a", b"1"])),
        Reply::ok()
    );
    assert_eq!(
        writer.apply(&command(&[b"SET", b"old:b", b"2"])),
        Reply::ok()
    );
    let old_snapshot = writer.snapshot_sorted();

    let mut source = OrderedArcticStore::new();
    assert_eq!(
        source.apply(&command(&[b"SET", b"new:a", b"10"])),
        Reply::ok()
    );
    assert_eq!(
        source.apply(&command(&[b"SET", b"new:b", b"20"])),
        Reply::ok()
    );
    let replacement = source.export_snapshot();
    let new_snapshot = source.snapshot_sorted();

    let reader = writer.reader();
    let pinned_old = reader.pin();
    let running = Arc::new(AtomicBool::new(true));
    let start = Arc::new(Barrier::new(READERS + 1));
    let mut workers = Vec::new();
    for _ in 0..READERS {
        let reader = reader.clone();
        let running = Arc::clone(&running);
        let start = Arc::clone(&start);
        let old_snapshot = old_snapshot.clone();
        let new_snapshot = new_snapshot.clone();
        workers.push(thread::spawn(move || {
            start.wait();
            while running.load(Ordering::Acquire) {
                let observed = reader.pin().snapshot_sorted();
                assert!(observed == old_snapshot || observed == new_snapshot);
            }
        }));
    }

    start.wait();
    writer.import_snapshot(&replacement).unwrap();
    let pinned_new = reader.pin();
    assert_eq!(pinned_new.epoch(), pinned_old.epoch() + 1);
    assert_eq!(pinned_new.snapshot_sorted(), new_snapshot);
    assert_eq!(pinned_old.snapshot_sorted(), old_snapshot);

    running.store(false, Ordering::Release);
    for worker in workers {
        worker.join().expect("snapshot observer panicked");
    }
}
