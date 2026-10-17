// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Startup steps that wait out a dependency that is not there yet, instead of
//! exiting.

use std::future::Future;
use std::time::Duration;

use backon::BackoffBuilder;
use scalo::sink_stack::SinkStackConfig;
use tokio_util::sync::CancellationToken;

/// Run `attempt` until it succeeds, or until `shutdown` fires.
///
/// Waits between failures on the `sink_stack` backoff from scalo's config
/// cascade, so a deployment's `sink_stack.min_backoff_ms` and `max_backoff_ms`
/// apply. `on_failure` gets each error, the attempt number and the wait before
/// the next attempt, and logs under the caller's own target.
///
/// Returns the value and the number of attempts it took, or `Ok(None)` when
/// `shutdown` fired first.
///
/// # Errors
///
/// Returns the first error `retryable` refuses, without waiting.
pub(crate) async fn retry_until_shutdown<T, E, F, Fut>(
    shutdown: &CancellationToken,
    mut attempt: F,
    retryable: impl Fn(&E) -> bool,
    mut on_failure: impl FnMut(&E, u32, Duration),
) -> Result<Option<(T, u32)>, E>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
{
    let backoff = SinkStackConfig::from_cascade();
    let ceiling = Duration::from_millis(backoff.max_backoff_ms);
    let mut schedule = backoff.backoff().without_max_times().build();
    let mut attempts: u32 = 0;
    loop {
        attempts = attempts.saturating_add(1);
        let outcome = tokio::select! {
            biased;
            () = shutdown.cancelled() => return Ok(None),
            outcome = attempt() => outcome,
        };
        let e = match outcome {
            Ok(value) => return Ok(Some((value, attempts))),
            Err(e) if retryable(&e) => e,
            Err(e) => return Err(e),
        };
        let retry_in = schedule.next().unwrap_or(ceiling);
        on_failure(&e, attempts, retry_in);
        tokio::select! {
            biased;
            () = shutdown.cancelled() => return Ok(None),
            () = tokio::time::sleep(retry_in) => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU32, Ordering};

    use super::*;

    #[tokio::test(start_paused = true)]
    async fn a_step_that_fails_twice_succeeds_on_the_third_attempt() {
        let calls = AtomicU32::new(0);
        let mut failures = Vec::new();
        let outcome = retry_until_shutdown(
            &CancellationToken::new(),
            || async {
                let n = calls.fetch_add(1, Ordering::Relaxed);
                if n < 2 { Err(n) } else { Ok("up") }
            },
            |_| true,
            |e, attempt, _| failures.push((*e, attempt)),
        )
        .await;

        assert_eq!(outcome, Ok(Some(("up", 3))));
        assert_eq!(failures, vec![(0, 1), (1, 2)]);
    }

    #[tokio::test(start_paused = true)]
    async fn an_error_it_will_not_retry_returns_at_once() {
        let mut failures = 0;
        let outcome: Result<Option<((), u32)>, &str> = retry_until_shutdown(
            &CancellationToken::new(),
            || async { Err("refused config") },
            |_| false,
            |_, _, _| failures += 1,
        )
        .await;

        assert_eq!(outcome, Err("refused config"));
        assert_eq!(failures, 0, "a refused error was counted as a retry");
    }

    #[tokio::test(start_paused = true)]
    async fn a_shutdown_during_the_wait_returns_none() {
        let shutdown = CancellationToken::new();
        let stop = shutdown.clone();
        let outcome: Result<Option<((), u32)>, &str> = retry_until_shutdown(
            &shutdown,
            || async { Err("broker down") },
            |_| true,
            |_, _, _| stop.cancel(),
        )
        .await;

        assert_eq!(outcome, Ok(None));
    }
}
