use std::sync::{Arc, Mutex, PoisonError, Weak};
use std::time::{Duration, Instant};
use std::{io, thread};

use linked_hash_map::LinkedHashMap;
use futures::{future, Future};

use middleware::session::{SessionError, SessionIdentifier};
use middleware::session::backend::{Backend, NewBackend, SessionFuture};
use redis;

use tokio_core::reactor::Handle;
// use redis_async::client;

use state::{self, FromState, State, StateData};

/// Defines the in-process memory based session storage.
///
/// This is the default implementation which is used by `NewSessionMiddleware::default()`
#[derive(Clone)]
pub struct RedisBackend {}

impl RedisBackend {
    /// Creates a new `MemoryBackend` where sessions expire and are removed after the `ttl` has
    /// elapsed.
    ///
    /// Alternately, `MemoryBackend::default()` creates a `MemoryBackend` with a `ttl` of one hour.
    ///
    /// ## Examples
    ///
    /// ```rust
    /// # extern crate gotham;
    /// # use std::time::Duration;
    /// # use gotham::middleware::session::{MemoryBackend, NewSessionMiddleware};
    /// # fn main() {
    /// NewSessionMiddleware::new(MemoryBackend::new(Duration::from_secs(3600)))
    /// # ;}
    /// ```
    // pub fn new(handle: &Handle) -> RedisBackend {
    pub fn new() -> RedisBackend {
        RedisBackend {}
    }
}

impl Default for RedisBackend {
    fn default() -> RedisBackend {
        RedisBackend::new()
    }
}

impl NewBackend for RedisBackend {
    type Instance = RedisBackend;

    fn new_backend(&self) -> io::Result<Self::Instance> {
        Ok(self.clone())
    }
}

impl Backend for RedisBackend {
    fn persist_session(
        &self,
        identifier: SessionIdentifier,
        content: &[u8],
    ) -> Result<(), SessionError> {
        Ok(())
    }

    fn read_session(&self, identifier: SessionIdentifier, state: &State) -> Box<SessionFuture> {
        let client = redis::Client::open("redis://127.0.0.1:6379").unwrap();
        let connect = client.get_async_connection(Handle::borrow_from(state));

        let a = redis::cmd("GET").arg(identifier.value).query_async(connect);
        // Handle::borrow_from(&state).spawn(a);
        // Handle::borrow_from(&state).spawn(
        //     connect.and_then(|conn| {
        //         redis::cmd("GET").arg(identifier.value).query_async(conn)
        //     })
        // );

        Box::new(future::ok(None))
    }

    fn drop_session(&self, identifier: SessionIdentifier) -> Result<(), SessionError> {
        Ok(())
    }
}

fn cleanup_loop(storage: Weak<Mutex<LinkedHashMap<String, (Instant, Vec<u8>)>>>, ttl: Duration) {
    loop {
        // If the original `Arc<_>` goes away, we don't need to keep sweeping the cache, because
        // it's gone too. We can bail out of this thread when the weak ref fails to upgrade.
        let storage = match storage.upgrade() {
            None => break,
            Some(storage) => storage,
        };

        let duration = match storage.lock() {
            Err(PoisonError { .. }) => break,
            Ok(mut storage) => cleanup_once(&mut storage, ttl),
        };

        if let Some(duration) = duration {
            thread::sleep(duration);
        }
    }
}

fn cleanup_once(
    storage: &mut LinkedHashMap<String, (Instant, Vec<u8>)>,
    ttl: Duration,
) -> Option<Duration> {
    match storage.front() {
        Some((_, &(instant, _))) => {
            let age = instant.elapsed();

            if age >= ttl {
                if let Some((key, _)) = storage.pop_front() {
                    trace!(" expired session {} and removed from MemoryBackend", key);
                }

                // We just removed one, so skip the sleep and check the next entry
                None
            } else {
                // Ensure to shrink the storage after a spike in sessions.
                //
                // Even with this, memory usage won't always drop back to pre-spike levels because
                // the OS can hang onto it.
                //
                // The arbitrary numbers here were chosen to avoid the resizes being extremely
                // frequent. Powers of 2 seemed like a reasonable idea, to let the optimiser
                // potentially shave off a few CPU cycles. Totally unscientific though.
                let cap = storage.capacity();
                let len = storage.len();

                if cap >= 65536 && cap / 8 > len {
                    storage.shrink_to_fit();

                    trace!(
                        " session backend had capacity {} and {} sessions, new capacity: {}",
                        cap,
                        len,
                        storage.capacity()
                    );
                }

                // Sleep until the next entry expires, but for at least 1 second
                Some(::std::cmp::max(ttl - age, Duration::from_secs(1)))
            }
        }
        // No sessions; sleep for the TTL, because that's the soonest we'll need to expire anything
        None => Some(ttl),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use futures::Future;
    use rand;

    #[test]
    fn cleanup_test() {
        let mut storage = LinkedHashMap::new();

        storage.insert(
            "abcd".to_owned(),
            (Instant::now() - Duration::from_secs(2), vec![]),
        );

        cleanup_once(&mut storage, Duration::from_secs(1));
        assert!(storage.is_empty());
    }

    #[test]
    fn cleanup_join_test() {
        let storage = Arc::new(Mutex::new(LinkedHashMap::new()));
        let weak = Arc::downgrade(&storage);

        let handle = thread::spawn(move || cleanup_loop(weak, Duration::from_millis(1)));

        drop(storage);
        handle.join().unwrap();
    }

    #[test]
    fn memory_backend_test() {
        let new_backend = MemoryBackend::new(Duration::from_millis(100));
        let bytes: Vec<u8> = (0..64).map(|_| rand::random()).collect();
        let identifier = SessionIdentifier {
            value: "totally_random_identifier".to_owned(),
        };

        new_backend
            .new_backend()
            .expect("can't create backend for write")
            .persist_session(identifier.clone(), &bytes[..])
            .expect("failed to persist");

        let received = new_backend
            .new_backend()
            .expect("can't create backend for read")
            .read_session(identifier.clone())
            .wait()
            .expect("no response from backend")
            .expect("session data missing");

        assert_eq!(bytes, received);
    }

    #[test]
    fn memory_backend_refresh_test() {
        let new_backend = MemoryBackend::new(Duration::from_millis(100));
        let bytes: Vec<u8> = (0..64).map(|_| rand::random()).collect();
        let identifier = SessionIdentifier {
            value: "totally_random_identifier".to_owned(),
        };
        let bytes2: Vec<u8> = (0..64).map(|_| rand::random()).collect();
        let identifier2 = SessionIdentifier {
            value: "another_totally_random_identifier".to_owned(),
        };

        let backend = new_backend
            .new_backend()
            .expect("can't create backend for write");

        backend
            .persist_session(identifier.clone(), &bytes[..])
            .expect("failed to persist");

        backend
            .persist_session(identifier2.clone(), &bytes2[..])
            .expect("failed to persist");

        {
            let mut storage = backend.storage.lock().expect("couldn't lock storage");
            assert_eq!(
                storage.front().expect("no front element").0,
                &identifier.value
            );

            assert_eq!(
                storage.back().expect("no back element").0,
                &identifier2.value
            );
        }

        backend
            .read_session(identifier.clone())
            .wait()
            .expect("failed to read session");

        {
            // Identifiers have swapped
            let mut storage = backend.storage.lock().expect("couldn't lock storage");
            assert_eq!(
                storage.front().expect("no front element").0,
                &identifier2.value
            );

            assert_eq!(
                storage.back().expect("no back element").0,
                &identifier.value
            );
        }
    }
}
