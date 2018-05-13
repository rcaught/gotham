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
        let ttl = self.ttl;

        Box::new(
            connect
                .and_then(move |conn| {
                    redis::cmd("SETEX")
                        .arg(identifier.value)
                        .arg(ttl.as_secs())
                        .arg(content)
                        .query_async(conn)
                })
                .map(|(_, val)| val)
                .map_err(|error| SessionError::Backend(format!("{}", error))),
        )
    }

    fn read_session(&self, identifier: SessionIdentifier, state: &State) -> Box<SessionFuture> {
        let client = redis::Client::open(self.url.as_ref()).unwrap();
        let connect = client.get_async_connection(Handle::borrow_from(state));

        Box::new(
            connect
                .and_then(|conn| redis::cmd("GET").arg(identifier.value).query_async(conn))
                .map(|(_, val)| val)
                .map_err(|error| SessionError::Backend(format!("{}", error))),
        )
    }

    fn drop_session(&self, identifier: SessionIdentifier, state: &State) -> Box<SessionUnitFuture> {
        let client = redis::Client::open(self.url.as_ref()).unwrap();
        let connect = client.get_async_connection(Handle::borrow_from(state));

        Box::new(
            connect
                .and_then(|conn| redis::cmd("DEL").arg(identifier.value).query_async(conn))
                .map(|(_, val)| val)
                .map_err(|error| SessionError::Backend(format!("{}", error))),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use futures::Future;
    use rand;

    #[test]
    fn redis_backend_test() {
        let new_backend = RedisBackend::default();
        let bytes: Vec<u8> = (0..64).map(|_| rand::random()).collect();
        let identifier = SessionIdentifier {
            value: "totally_random_identifier".to_owned(),
        };
        let state = State::new();

        new_backend
            .new_backend()
            .expect("can't create backend for write")
            .persist_session(identifier.clone(), bytes, &state);

        let received = new_backend
            .new_backend()
            .expect("can't create backend for read")
            .read_session(identifier.clone(), &state)
            .wait()
            .expect("no response from backend")
            .expect("session data missing");

        assert_eq!(bytes, received);
    }

    #[test]
    fn redis_backend_refresh_test() {
        let new_backend = RedisBackend::default();
        let bytes: Vec<u8> = (0..64).map(|_| rand::random()).collect();
        let identifier = SessionIdentifier {
            value: "totally_random_identifier".to_owned(),
        };
        let bytes2: Vec<u8> = (0..64).map(|_| rand::random()).collect();
        let identifier2 = SessionIdentifier {
            value: "another_totally_random_identifier".to_owned(),
        };
        let state = State::new();

        let backend = new_backend
            .new_backend()
            .expect("can't create backend for write");

        backend.persist_session(identifier.clone(), bytes, &state);

        backend.persist_session(identifier2.clone(), bytes2, &state);

        {
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
            .read_session(identifier.clone(), &state)
            .wait()
            .expect("failed to read session");

        {
            // Identifiers have swapped
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
