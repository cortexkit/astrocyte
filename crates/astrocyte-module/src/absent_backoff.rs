//! Retry intervals for optional publishers the daemon reports absent.
//!
//! The daemon counts every refused route open, so asking an absent module on
//! every tick would log a refusal forever. These publishers keep their records
//! until astrocyte reads them, so a later first read loses nothing.

const FIRST_RETRY_MS: i64 = 60_000;
const MAX_RETRY_MS: i64 = 600_000;

#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct AbsentBackoff {
    /// When the publisher was last found absent, and how long to wait from then.
    waiting: Option<(i64, i64)>,
}

impl AbsentBackoff {
    pub(crate) fn is_waiting(&self) -> bool {
        self.waiting.is_some()
    }

    pub(crate) fn may_try(&self, now: i64) -> bool {
        self.waiting
            .is_none_or(|(since, wait)| now.saturating_sub(since) >= wait)
    }

    pub(crate) fn absent(&mut self, now: i64) {
        let wait = self.waiting.map_or(FIRST_RETRY_MS, |(_, wait)| {
            wait.saturating_mul(2).min(MAX_RETRY_MS)
        });
        self.waiting = Some((now, wait));
    }

    pub(crate) fn present(&mut self) {
        self.waiting = None;
    }
}
