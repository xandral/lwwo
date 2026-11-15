//! Alerts: the master says so when something needs a person.
//!
//! Two things are worth telling: tasks that have been waiting a long time
//! for a worker, which usually means a misspelled label or a worker that
//! is gone, and runs that failed. Both are off unless `[alerts]` names a
//! webhook. An alert is one HTTP POST with a JSON body, sent once, after
//! the database commit it is about. It is not stored: an alert that cannot
//! be delivered is written to the master's log and dropped, and an alert
//! that was due while the master was down is not sent later.

use std::time::Duration;

use serde_json::Value;
use tracing::{debug, warn};

/// Attempts at delivering one alert.
const ATTEMPTS: u32 = 3;

/// Where alerts go.
#[derive(Debug, Clone)]
pub(crate) struct Webhook {
    client: reqwest::Client,
    url: String,
}

impl Webhook {
    pub(crate) fn new(url: String) -> Self {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .unwrap_or_default();
        Self { client, url }
    }

    /// Sends `payload` in the background: the master's loop never waits
    /// for the receiver.
    pub(crate) fn send(&self, payload: Value) {
        let webhook = self.clone();
        tokio::spawn(async move {
            let mut wait = Duration::from_secs(1);
            for attempt in 1..=ATTEMPTS {
                match webhook
                    .client
                    .post(&webhook.url)
                    .json(&payload)
                    .send()
                    .await
                {
                    Ok(response) if response.status().is_success() => {
                        debug!("alert delivered");
                        return;
                    }
                    // The URL is a secret: it never goes into the log.
                    Ok(response) => warn!(
                        "the alert webhook answered {} (attempt {attempt} of {ATTEMPTS})",
                        response.status()
                    ),
                    Err(error) => warn!(
                        "cannot reach the alert webhook (attempt {attempt} of {ATTEMPTS}): {}",
                        error.without_url()
                    ),
                }
                tokio::time::sleep(wait).await;
                wait *= 4;
            }
            warn!("an alert was dropped: the webhook did not take it");
        });
    }
}

/// A duration as people write it: `90s`, `15m`, `2h`.
pub(crate) fn duration_text(duration: Duration) -> String {
    let seconds = duration.as_secs();
    if seconds >= 3_600 && seconds % 3_600 == 0 {
        format!("{}h", seconds / 3_600)
    } else if seconds >= 60 && seconds % 60 == 0 {
        format!("{}m", seconds / 60)
    } else {
        format!("{seconds}s")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn durations_are_written_as_in_the_configuration() {
        assert_eq!(duration_text(Duration::from_secs(900)), "15m");
        assert_eq!(duration_text(Duration::from_secs(7_200)), "2h");
        assert_eq!(duration_text(Duration::from_secs(90)), "90s");
        assert_eq!(duration_text(Duration::from_secs(1)), "1s");
    }
}
