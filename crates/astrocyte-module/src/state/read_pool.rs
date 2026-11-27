use std::sync::{Condvar, Mutex};

use rusqlite::Connection;

pub(super) struct ReadPool {
    idle: Mutex<Vec<Connection>>,
    available: Condvar,
}

impl ReadPool {
    pub(super) fn new(connections: [Connection; 2]) -> Self {
        Self {
            idle: Mutex::new(Vec::from(connections)),
            available: Condvar::new(),
        }
    }

    pub(super) fn with<T>(&self, read: impl FnOnce(&Connection) -> T) -> T {
        // Hold the pool lock only for checkout and return, not for the read.
        let idle = self
            .idle
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let mut idle = self
            .available
            .wait_while(idle, |connections| connections.is_empty())
            .unwrap_or_else(|poison| poison.into_inner());
        let loan = ReaderLoan {
            pool: self,
            conn: idle.pop(),
        };
        drop(idle);
        read(loan.conn.as_ref().expect("an idle reader is available"))
    }
}

struct ReaderLoan<'a> {
    pool: &'a ReadPool,
    conn: Option<Connection>,
}

impl Drop for ReaderLoan<'_> {
    fn drop(&mut self) {
        // A panicking read drops its transaction before returning the reader.
        if let Some(conn) = self.conn.take() {
            self.pool
                .idle
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .push(conn);
            self.pool.available.notify_one();
        }
    }
}
