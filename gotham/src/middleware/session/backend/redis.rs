use std::io;
use std::time::Duration;

use futures::Future;

use middleware::session::backend::{Backend, NewBackend, SessionFuture, SessionUnitFuture};
use middleware::session::{SessionError, SessionIdentifier};
use redis;

use tokio_core::reactor::Handle;

use state::{FromState, State};

/// Defines the in-process memory based session storage.
///
/// This is the default implementation which is used by `NewSessionMiddleware::default()`
#[derive(Clone)]
pub struct RedisBackend {
    url: String,
    ttl: Duration,
}

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
    pub fn new(url: String, ttl: Duration) -> RedisBackend {
        RedisBackend { url, ttl }
    }
}

impl Default for RedisBackend {
    fn default() -> RedisBackend {
        RedisBackend::new(
            "redis://127.0.0.1:6379".to_owned(),
            Duration::from_secs(3600),
        )
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
        content: Vec<u8>,
        state: &State,
    ) -> Box<SessionUnitFuture> {
        let client = redis::Client::open(self.url.as_ref()).unwrap();
        let connect = client.get_async_connection(Handle::borrow_from(state));

        Box::new(
            connect
                .and_then(|conn| {
                    redis::cmd("SETEX")
                        .arg(identifier.value)
                        .arg(content)
                        .arg(self.ttl.as_secs())
                        .query_async(conn)
                })
                .map(|(_, val)| val)
                .map_err(|_| SessionError::Backend("cheese".to_owned())),
        )
    }

    fn read_session(&self, identifier: SessionIdentifier, state: &State) -> Box<SessionFuture> {
        let client = redis::Client::open(self.url.as_ref()).unwrap();
        let connect = client.get_async_connection(Handle::borrow_from(state));

        Box::new(
            connect
                .and_then(|conn| redis::cmd("GET").arg(identifier.value).query_async(conn))
                .map(|(_, val)| val)
                .map_err(|_| SessionError::Backend("cheese".to_owned())),
        )
    }

    fn drop_session(&self, identifier: SessionIdentifier, state: &State) -> Box<SessionUnitFuture> {
        let client = redis::Client::open(self.url.as_ref()).unwrap();
        let connect = client.get_async_connection(Handle::borrow_from(state));

        Box::new(
            connect
                .and_then(|conn| redis::cmd("DEL").arg(identifier.value).query_async(conn))
                .map(|(_, val)| val)
                .map_err(|_| SessionError::Backend("cheese".to_owned())),
        )
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
    fn redis_backend_test() {
        let new_backend = RedisBackend::new("".to_owned(), Duration::from_seconds(1));
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
