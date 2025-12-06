use std::{
	path::{Path, PathBuf},
	sync::{
		Arc,
		atomic::{AtomicUsize, Ordering},
	},
};

use fs2::FileExt;
use serde::{Serialize, de::Deserialize};

pub use dirtyqueue_derive::DirtyQueue;

const HINT_FILE: &str = "hint";

type SafeUsize = Arc<AtomicUsize>;
type StdResult<T> = std::result::Result<T, Error>;

pub static mut DIRECTORY_HASH_LEVELS: usize = 1;

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct Error(Arc<String>);

impl<T> From<T> for Error
where
	T: std::fmt::Display,
{
	#[inline]
	fn from(value: T) -> Self {
		Self(Arc::new(value.to_string()))
	}
}

#[derive(Debug, Clone)]
pub enum QueueDimension {
	Head(usize),
	Tail(usize),
	Closing,
}

#[inline]
fn next_with_overflow(u: usize) -> usize {
	if u == usize::MAX { 0 } else { u + 1 }
}

#[inline]
fn advance(u: &SafeUsize) -> StdResult<usize> {
	let mut cur = u.load(Ordering::SeqCst);
	while let Err(ret) = u.compare_exchange(
		cur,
		next_with_overflow(cur),
		Ordering::SeqCst,
		Ordering::SeqCst,
	) {
		cur = ret;
	}

	Ok(next_with_overflow(cur))
}

fn hash_filename(path: &Path, count: usize) -> PathBuf {
	let mut path = path.to_path_buf();

	let s = count.to_string();
	let mut x = 0;
	let s_len = s.len();

	let mut added = 0;

	while x < s_len && added < unsafe { DIRECTORY_HASH_LEVELS } {
		let slice = if x + 2 > s_len {
			&format!("0{}", &s[x..s_len])
		} else {
			&s[x..x + 2]
		};

		path = path.join(slice);
		x += 2;
		added += 1;
	}

	for _ in added..unsafe { DIRECTORY_HASH_LEVELS } {
		path = path.join("00");
	}

	path.join(s).to_path_buf()
}

pub trait Keyed: Sized {
	fn key(&self) -> usize;
	fn set_key(&mut self, key: usize) -> usize;
	fn initialized(&self) -> bool;
}

pub trait IO: Serialize + for<'de> Deserialize<'de> {
	fn read_from(filename: &PathBuf) -> StdResult<Self> {
		let mut f = std::fs::OpenOptions::new().read(true).open(filename)?;
		Ok(ciborium::from_reader(&mut f)?)
	}

	#[inline]
	fn finished(&self, filename: &PathBuf) -> StdResult<()> {
		std::fs::remove_file(filename)?;
		Ok(())
	}

	fn write_to(&self, path: &PathBuf) -> StdResult<()> {
		let parent = path.parent().map(|x| x.to_str().unwrap()).unwrap_or("/");
		if !std::fs::exists(parent)? {
			std::fs::create_dir_all(parent)?;
		}

		let f = std::fs::OpenOptions::new()
			.create(true)
			.write(true)
			.open(path)?;

		ciborium::into_writer(self, f)?;
		Ok(())
	}
}

#[derive(Debug, Clone)]
pub struct DirtyQueue<T>
where
	T: IO + Keyed + Clone + Sync,
{
	root: PathBuf,
	head: SafeUsize,
	tail: SafeUsize,
	sender: std::sync::mpsc::Sender<QueueDimension>,
	_t: std::marker::PhantomData<T>,
}

impl<T> Drop for DirtyQueue<T>
where
	T: IO + Keyed + Clone + Sync,
{
	fn drop(&mut self) {
		self.sender.send(QueueDimension::Closing).unwrap();
	}
}

impl<T> DirtyQueue<T>
where
	T: IO + Keyed + Clone + Sync,
{
	pub fn new(path: impl AsRef<Path>) -> StdResult<Self> {
		let pbuf = path.as_ref().to_path_buf();

		if !std::fs::exists(&pbuf)? {
			std::fs::create_dir_all(&pbuf)?;
		}

		let (s, r) = std::sync::mpsc::channel();
		let (head, tail) = Self::take_hint(path.as_ref())?;

		let p = pbuf.clone();
		std::thread::spawn(move || Self::write_hints(&p, r));

		Ok(Self {
			root: pbuf,
			head: Arc::new(AtomicUsize::from(head)),
			tail: Arc::new(AtomicUsize::from(tail)),
			sender: s,
			_t: Default::default(),
		})
	}

	#[inline]
	pub fn head(&self) -> StdResult<usize> {
		Ok(self.head.load(Ordering::SeqCst))
	}

	#[inline]
	pub fn advance_head(&self) -> StdResult<usize> {
		advance(&self.head)
	}

	#[inline]
	pub fn advance_tail(&self) -> StdResult<usize> {
		advance(&self.tail)
	}

	#[inline]
	pub fn tail(&self) -> StdResult<usize> {
		Ok(self.tail.load(Ordering::SeqCst))
	}

	#[inline]
	pub fn queue_size(&self) -> StdResult<usize> {
		Ok(self.tail()?.abs_diff(self.head()?))
	}

	pub fn push(&self, mut obj: T) -> StdResult<usize> {
		let idx = self.advance_tail()?;
		obj.set_key(idx);
		obj.write_to(&hash_filename(&self.root, idx))?;

		self.sender.send(QueueDimension::Tail(idx))?;

		Ok(idx)
	}

	pub fn shift(&self) -> StdResult<T> {
		let idx = self.advance_head()?;
		let mut obj = T::read_from(&hash_filename(&self.root, idx))?;
		obj.set_key(idx);
		self.sender.send(QueueDimension::Head(idx))?;

		Ok(obj)
	}

	pub fn finished(&self, obj: T) -> StdResult<T> {
		if !obj.initialized() {
			return Ok(obj);
		}

		obj.finished(&hash_filename(&self.root, obj.key()))?;
		Ok(obj)
	}

	#[inline]
	pub fn shift_finished(&self) -> StdResult<T> {
		self.finished(self.shift()?)
	}

	fn write_hints(root: &Path, receiver: std::sync::mpsc::Receiver<QueueDimension>) {
		fn juggle_write(root: &Path, filename: &str, data: &[usize]) {
			let mut f = std::fs::OpenOptions::new()
				.create(true)
				.write(true)
				.open(root.join(&filename))
				.expect("Opening temporary hint file for writing");
			f.lock_exclusive().expect("Locking temporary hint file");

			ciborium::into_writer(&data, &mut f).expect("Serializing temporary hint file payload");

			let hint = std::fs::OpenOptions::new()
				.create(true)
				.write(true)
				.open(root.join(HINT_FILE))
				.expect("Opening hint file for writing");

			hint.lock_exclusive().expect("Locking hint file");

			let _ = std::fs::rename(root.join(&filename), root.join(HINT_FILE));
		}

		if !std::fs::exists(root).expect("Checking root directory existence") {
			std::fs::create_dir_all(root).expect("Creating root_ directory");
		}

		let mut last_write = std::time::Instant::now();
		let mut last_head = 0;
		let mut last_tail = 0;

		loop {
			if let Ok(qd) = receiver.recv() {
				let (filename, data) = match qd {
					QueueDimension::Head(head) => (
						format!("{}.head.{}.tmp", HINT_FILE, head),
						vec![head, last_tail],
					),
					QueueDimension::Tail(tail) => (
						format!("{}.tail.{}.tmp", HINT_FILE, tail),
						vec![last_head, tail],
					),
					QueueDimension::Closing => {
						eprintln!("completing: {} {}", last_head, last_tail);
						let filename = format!("{}.{}.{}.tmp", HINT_FILE, last_head, last_tail);
						juggle_write(root, &filename, &vec![last_head, last_tail]);
						return;
					}
				};

				if std::time::Instant::now() - last_write > std::time::Duration::from_nanos(500) {
					juggle_write(root, &filename, &data);
					last_write = std::time::Instant::now();
				}

				last_head = data[0];
				last_tail = data[1];
			}
		}
	}

	fn take_hint(path: impl AsRef<Path>) -> StdResult<(usize, usize)> {
		match std::fs::OpenOptions::new()
			.read(true)
			.open(path.as_ref().to_path_buf().join(HINT_FILE))
		{
			Ok(mut f) => {
				let v: Vec<usize> = ciborium::from_reader(&mut f)?;

				Ok((v[0], v[1]))
			}
			Err(_) => Ok((0, 0)),
		}
	}
}

#[cfg(test)]
mod tests {
	use std::{
		path::PathBuf,
		str::FromStr,
		sync::{Arc, atomic::AtomicBool},
		usize,
	};

	use fancy_duration::AsFancyDuration;
	use serde::{Deserialize, Serialize};

	use crate::*;

	// struct used for payload in tests
	#[derive(Debug, Clone, Serialize, Deserialize, Default, DirtyQueue)]
	struct Thing {
		x: usize,
		#[serde(skip)]
		_key: Option<usize>,
	}

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

	#[test]
	fn test_hash_filename() {
		const LONG_PATH: &str =
			"/complicated/deep/path/like/seriously/bro/its/looooooooooooooooooong/";

		let roots = vec!["/", LONG_PATH];
		let usizes = vec![0, 8675309, usize::MAX];

		let biggest = &usize::MAX.to_string()[0..2];

		let results = vec![
			vec![
				"/00/0".to_string(),
				"/86/8675309".to_string(),
				format!("/{}/", biggest) + &usize::MAX.to_string(),
			],
			vec![
				LONG_PATH.to_string() + "00/0",
				LONG_PATH.to_string() + "86/8675309",
				LONG_PATH.to_string() + &format!("{}/", biggest) + &usize::MAX.to_string(),
			],
		];

		for (u, size) in usizes.iter().enumerate() {
			for (r, root) in roots.iter().enumerate() {
				assert_eq!(
					PathBuf::from_str(&results[r][u]).unwrap(),
					hash_filename(&PathBuf::from_str(root).unwrap(), *size)
				)
			}
		}
	}

	#[test]
	fn test_construction_big() {
		const SIZE: usize = 100000;

		let dir = tempfile::tempdir().unwrap();
		eprintln!("\nbig dir: {}", dir.path().display());

		let queue: DirtyQueue<Thing> = DirtyQueue::new(dir.path()).unwrap();

		let start = std::time::Instant::now();
		for x in 0..SIZE {
			let res = queue.push(Thing {
				x,
				..Default::default()
			});

			assert!(res.is_ok(), "{} | {:?}", x, res);
		}

		eprintln!(
			"\nPush duration: (size: {}): {}",
			SIZE,
			(std::time::Instant::now() - start).fancy_duration()
		);

		let start = std::time::Instant::now();
		for x in 0..SIZE {
			let res = queue.shift();
			assert!(res.is_ok(), "{} | {:?}", x, res);
			assert_eq!(res.unwrap().x, x, "{}", x);
		}

		eprintln!(
			"\nShift duration (size: {}): {}",
			SIZE,
			(std::time::Instant::now() - start).fancy_duration()
		);

		let queue: DirtyQueue<Thing> = DirtyQueue::new(dir.path()).unwrap();
		eprintln!("big queue size: {}", queue.queue_size().unwrap());
		assert!(queue.shift().is_err())
	}

	#[test]
	fn test_construction_overflow() {
		let dir = tempfile::tempdir().unwrap();

		let queue: DirtyQueue<Thing> = DirtyQueue::new(dir.path()).unwrap();
		queue.head.store(usize::MAX, Ordering::SeqCst);
		queue.tail.store(usize::MAX, Ordering::SeqCst);

		for x in 0..100 {
			let res = queue.push(Thing {
				x,
				..Default::default()
			});

			assert!(res.is_ok(), "{} | {:?}", x, res);
		}

		for x in 0..100 {
			let res = queue.shift();
			assert!(res.is_ok(), "{} | {:?}", x, res);
			assert_eq!(res.unwrap().x, x, "{}", x);
		}
	}

	#[test]
	fn test_construction_use() {
		let dir = tempfile::tempdir().unwrap();

		let queue: DirtyQueue<Thing> = DirtyQueue::new(dir.path()).unwrap();

		for x in 0..100 {
			let res = queue.push(Thing {
				x,
				..Default::default()
			});

			assert!(res.is_ok(), "{} | {:?}", x, res);
		}

		for x in 0..100 {
			let res = queue.shift();
			assert!(res.is_ok(), "{} | {:?}", x, res);
			assert_eq!(res.unwrap().x, x, "{}", x);
		}
	}

	#[tokio::test]
	async fn test_tokio() {
		const SIZE: usize = 100000;
		let dir = tempfile::tempdir().unwrap();
		eprintln!("\ntokio dir: {}", dir.path().display());

		let queue = DirtyQueue::<Thing>::new(dir.path()).unwrap();

		let bool: Arc<AtomicBool> = Default::default();

		let q = queue.clone();
		let b = bool.clone();
		tokio::spawn(async move {
			let start = std::time::Instant::now();
			for x in 0..SIZE {
				let res = q.push(Thing {
					x,
					..Default::default()
				});
				assert!(res.is_ok(), "{} | {:?}", x, res);
			}
			eprintln!(
				"\ntokio push round trip: {}",
				(std::time::Instant::now() - start).fancy_duration()
			);
			b.store(true, Ordering::SeqCst);
		});

		while !bool.load(Ordering::SeqCst) {
			tokio::time::sleep(std::time::Duration::from_nanos(100)).await;
		}

		let mut count = 1;
		let start = std::time::Instant::now();
		while let Ok(res) = queue.shift() {
			assert_eq!(res.key(), count);
			count += 1;
		}

		eprintln!(
			"\ntokio shift round trip: {}",
			(std::time::Instant::now() - start).fancy_duration()
		);

		assert_eq!(count - 1, SIZE);

		let queue: DirtyQueue<Thing> = DirtyQueue::new(dir.path()).unwrap();
		eprintln!("tokio queue size: {}", queue.queue_size().unwrap());
		assert!(queue.shift().is_err())
	}

	#[test]
	fn test_thread() {
		const SIZE: usize = 100000;
		let dir = tempfile::tempdir().unwrap();
		eprintln!("\nthread dir: {}", dir.path().display());

		let queue: DirtyQueue<Thing> = DirtyQueue::new(dir.path()).unwrap();
		let bool: Arc<AtomicBool> = Default::default();

		let q = queue.clone();
		let b = bool.clone();
		std::thread::spawn(move || {
			let start = std::time::Instant::now();
			for x in 0..SIZE {
				let res = q.push(Thing {
					x,
					..Default::default()
				});
				assert!(res.is_ok(), "{} | {:?}", x, res);
			}
			eprintln!(
				"\nthread push round trip: {}",
				(std::time::Instant::now() - start).fancy_duration()
			);
			b.store(true, Ordering::SeqCst);
		});

		while !bool.load(Ordering::SeqCst) {
			std::thread::sleep(std::time::Duration::from_nanos(100));
		}

		// internally, the queue's keys start counting at 1, not 0.
		let mut count = 1;
		let start = std::time::Instant::now();
		while let Ok(res) = queue.shift() {
			assert_eq!(res.key(), count);
			count += 1;
		}

		eprintln!(
			"\nthread shift round trip: {}",
			(std::time::Instant::now() - start).fancy_duration()
		);

		assert_eq!(count - 1, SIZE);

		let queue: DirtyQueue<Thing> = DirtyQueue::new(dir.path()).unwrap();
		eprintln!("thread queue size: {}", queue.queue_size().unwrap());
		assert!(queue.shift().is_err())
	}
}
