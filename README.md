# `dirtyqueue`: a durable queue built out of a hashed filesystem

This library implements a queue over a type that implements `serde` traits and serializes them to the filesystem when pushed, and deserializes them when shifted. It keeps track of the queue by way of a pair of atomic usizes which are programmed to overflow gracefully, meaning the queue size can reach usize::MAX while using almost no memory. The queue can also be restarted, programming a well-managed hint file that gives it parameters it can start with, in the event of program termination or system halt.

## Details

The queue implementation uses std::io/fs for file access, and `fs2` for flock, which it uses to manage the hint file only. It is tested against both a simple threaded version and tokio. An asynchronous I/O implementation is planned.

It expects that your serialized types implement two traits which come with the library; `Keyed` and `IO`, the latter of which is covered by `derive(DirtyQueue)`. `Keyed` requires storage, but you can `#[serde(skip)]` it gracefully. The queue is Debug, Clone, and Send, but not Sync. If you wish to use it in certain scenarios where it needs to cross thread boundaries, you may need to wrap it in a mutex, but it is not necessary in any of the tests.

## Performance

A lot of time has been spent trying to improve performance and reliability.

It's pretty fast! Iterating over 100k items containing 2 usizes apiece, single threaded, on a Ryzen 5900x and Samsung 980 Pro: roughly 2 seconds for push into the queue another 2 for shifting off it.

The hashed directory structure is single level, dual digit (based on the largest two single-digit base-10 numbers in the number) and is programmed to fill breadth-first. If you have really large queues, there is a static you can change (with unsafe) that creates a multi-level directory structure based on successive dual digit pairs. Both file writes and access is really fast with the current setting locally.

The payload is stored as CBOR for minimum write length for generic types and a faster serialization time. Locking is only flock and only on one file, but it currently acquires this lock on every queue modification; this is something I'm actively trying to eliminate. Durable writing of the hint file uses a temporary file (in case of abrupt halt) and then removes the original hint file, which is locked, and hard links the temporary file to it. This avoids additional expense brought just by renaming the file.

## Benefits and Disadvantages

The filesystem approach has many benefits, but is not for everyone:

- Easy to archive gracefully (disk snaps will always be reliable)
- Easy to write secondary processing programs to examine the queue, such as manually deserializing items.
- Next to impossible to use over a network
- If you don't need durability, stay off the filesystem if you want to dramatically increase performance.

## Example

```rust
use dirtyqueue::*;
use serde::{Serialize, Deserialize};

// the DirtyQueue derive will implement file management traits to control your
// object on the filesystem.
#[derive(Debug, Clone, Serialize, Deserialize, Default, DirtyQueue)]
struct Thing {
    x: usize,
    // Our key is modified every time it runs through a queue processing
    // function. We implement `Keyed` below to let the queue manage it.
    #[serde(skip)]
    _key: Option<usize>,
}

// this just juggles the key for a specific object.
impl Keyed for Thing {
    fn initialized(&self) -> bool {
        self._key.is_some()
    }

    fn key(&self) -> usize {
        self._key.unwrap_or_default()
    }

    fn set_key(&mut self, key: usize) -> usize {
        let old = self.key();
        self._key = Some(key);
        old
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir().unwrap();

    let queue = DirtyQueue::new(dir.path()).unwrap();

    for x in 0..100 {
        queue.push(Thing {
            x,
            ..Default::default()
        })?;
    }

    for x in 0..100 {
        let item = queue.shift()?;

        // key will always be in order
        //
        // x will not be in situations where multiple threads or futures are
        // writing to the queue simultaneously.
        eprintln!("key: {}, x: {}", item.key(), item.x);
    }

    Ok(())
}
```

## To do:

- Asynchronous I/O Implementation
- Examples

## License

MIT

## Author

Erik Hollensbe <git@hollensbe.org>
