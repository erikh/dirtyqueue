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

impl From<Box<dyn std::error::Error>> for Error {
	#[inline]
	fn from(value: Box<dyn std::error::Error>) -> Self {
		Self(Arc::new(value.to_string()))
	}
}

impl From<ciborium::de::Error<std::io::Error>> for Error {
	#[inline]
	fn from(value: ciborium::de::Error<std::io::Error>) -> Self {
		Self(Arc::new(value.to_string()))
	}
}

impl From<ciborium::ser::Error<std::io::Error>> for Error {
	#[inline]
	fn from(value: ciborium::ser::Error<std::io::Error>) -> Self {
		Self(Arc::new(value.to_string()))
	}
}

impl From<std::io::Error> for Error {
	#[inline]
	fn from(value: std::io::Error) -> Self {
		Self(Arc::new(value.to_string()))
	}
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
	_t: std::marker::PhantomData<T>,
}

impl<T> DirtyQueue<T>
where
	T: IO + Keyed + Clone + Sync,
{
	pub fn new(path: impl AsRef<Path>) -> StdResult<Self> {
		if !std::fs::exists(path.as_ref())? {
			std::fs::create_dir_all(path.as_ref())?;
		}

		let (head, tail) = Self::take_hint(path.as_ref())?;

		Ok(Self {
			root: path.as_ref().to_path_buf(),
			head: Arc::new(AtomicUsize::from(head)),
			tail: Arc::new(AtomicUsize::from(tail)),
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
		self.write_hint(self.head()?, idx)?;

		Ok(idx)
	}

	pub fn shift(&self) -> StdResult<T> {
		let idx = self.advance_head()?;
		let mut obj = T::read_from(&hash_filename(&self.root, idx))?;
		obj.set_key(idx);
		self.write_hint(obj.key(), self.tail()?)?;

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

	fn write_hint(&self, next: usize, last: usize) -> StdResult<()> {
		if !std::fs::exists(&self.root)? {
			std::fs::create_dir_all(&self.root)?;
		}

		let tmp = format!("{}.tmp", HINT_FILE);

		let mut f = std::fs::OpenOptions::new()
			.create(true)
			.write(true)
			.open(self.root.join(&tmp))?;

		f.lock_exclusive()?;

		// if this check fails, we've raced waiting for a lock and someone else won; exit cleanly so
		// we don't cause more trouble.
		ciborium::into_writer(&vec![next, last], &mut f)?;
		let _ = std::fs::remove_file(self.root.join(HINT_FILE));
		std::fs::hard_link(self.root.join(&tmp), self.root.join(HINT_FILE))?;

		Ok(())
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

		let queue = DirtyQueue::new(dir.path()).unwrap();

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
		assert!(queue.shift().is_err())
	}

	#[test]
	fn test_construction_overflow() {
		let dir = tempfile::tempdir().unwrap();

		let queue = DirtyQueue::new(dir.path()).unwrap();
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

		let queue = DirtyQueue::new(dir.path()).unwrap();

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

		let queue = DirtyQueue::new(dir.path()).unwrap();

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
		assert!(queue.shift().is_err())
	}

	#[test]
	fn test_thread() {
		const SIZE: usize = 100000;
		let dir = tempfile::tempdir().unwrap();
		eprintln!("\nthread dir: {}", dir.path().display());

		let queue = DirtyQueue::new(dir.path()).unwrap();
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
		assert!(queue.shift().is_err())
	}
}
