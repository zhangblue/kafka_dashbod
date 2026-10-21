use serde::{Deserialize, Serialize};
use std::time::{Duration, Instant};

pub const MIN_GROWTH_SECONDS: i64 = 10;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FeishuAlertRuleInput {
    pub id: Option<String>,
    pub group_id: String,
    pub topic: String,
    pub lag_threshold: i64,
    pub growth_seconds: i64,
    pub enabled: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, sqlx::FromRow)]
#[serde(rename_all = "camelCase")]
pub struct FeishuAlertRule {
    pub id: String,
    pub group_id: String,
    pub topic: String,
    pub lag_threshold: i64,
    pub growth_seconds: i64,
    pub enabled: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FeishuAlertSettings {
    pub access_token_configured: bool,
    pub masked_access_token: Option<String>,
    pub rules: Vec<FeishuAlertRule>,
}

/// Internal worker configuration; deliberately not serializable.
#[derive(Clone)]
pub struct EnabledAlertCluster {
    pub cluster_id: String,
    pub access_token: String,
    pub rules: Vec<FeishuAlertRule>,
    pub revision: String,
}

impl std::fmt::Debug for EnabledAlertCluster {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("EnabledAlertCluster([REDACTED])")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AlertDecision {
    None,
    Send,
}

/// Pure state for one rule. A valid recovery is the only way to rearm a
/// successfully notified or exhausted alert; unknown samples only break growth.
#[derive(Debug, Default)]
pub struct AlertState {
    candidate: Option<GrowthCandidate>,
    delivery: DeliveryState,
}

#[derive(Debug)]
struct GrowthCandidate {
    started_at: Instant,
    last_lag: i64,
    has_grown: bool,
}

#[derive(Debug, Default, Clone, Copy)]
enum DeliveryState {
    #[default]
    Ready,
    InFlight {
        failed_attempts: u8,
    },
    RetryAt {
        failed_attempts: u8,
        at: Instant,
    },
    Notified,
    Suppressed,
}

impl AlertState {
    pub fn observe(
        &mut self,
        now: Instant,
        total_lag: Option<i64>,
        threshold: i64,
        duration: Duration,
    ) -> AlertDecision {
        let Some(total_lag) = total_lag else {
            self.candidate = None;
            return AlertDecision::None;
        };
        if total_lag <= threshold {
            *self = Self::default();
            return AlertDecision::None;
        }

        let candidate = self.candidate.get_or_insert(GrowthCandidate {
            started_at: now,
            last_lag: total_lag,
            has_grown: false,
        });
        if total_lag < candidate.last_lag {
            *candidate = GrowthCandidate {
                started_at: now,
                last_lag: total_lag,
                has_grown: false,
            };
        } else {
            candidate.has_grown |= total_lag > candidate.last_lag;
            candidate.last_lag = total_lag;
        }
        if !candidate.has_grown || now.saturating_duration_since(candidate.started_at) < duration {
            return AlertDecision::None;
        }

        let failed_attempts = match self.delivery {
            DeliveryState::Ready => 0,
            DeliveryState::RetryAt {
                failed_attempts,
                at,
            } if now >= at => failed_attempts,
            _ => return AlertDecision::None,
        };
        self.delivery = DeliveryState::InFlight { failed_attempts };
        AlertDecision::Send
    }

    /// Complete the outstanding `Send`; the retry delay starts at this result.
    pub fn record_attempt(&mut self, now: Instant, succeeded: bool) {
        let DeliveryState::InFlight { failed_attempts } = self.delivery else {
            return;
        };
        self.delivery = if succeeded {
            DeliveryState::Notified
        } else if failed_attempts >= 2 {
            DeliveryState::Suppressed
        } else {
            DeliveryState::RetryAt {
                failed_attempts: failed_attempts + 1,
                at: now + Duration::from_secs(30),
            }
        };
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};

    enum Step {
        Observe(u64, Option<i64>, AlertDecision),
        Attempt(u64, bool),
    }

    fn check(steps: &[Step]) {
        let start = Instant::now();
        let mut state = AlertState::default();
        for step in steps {
            match *step {
                Step::Observe(seconds, lag, expected) => assert_eq!(
                    state.observe(
                        start + Duration::from_secs(seconds),
                        lag,
                        5_000,
                        Duration::from_secs(10),
                    ),
                    expected,
                    "at {seconds}s with total Lag {lag:?}",
                ),
                Step::Attempt(seconds, succeeded) => {
                    state.record_attempt(start + Duration::from_secs(seconds), succeeded);
                }
            }
        }
    }

    #[test]
    fn duration_starts_at_first_sample_strictly_above_threshold() {
        use AlertDecision::{None as Quiet, Send};
        check(&[
            Step::Observe(0, Some(5_000), Quiet),
            Step::Observe(5, Some(5_100), Quiet),
            Step::Observe(10, Some(5_200), Quiet),
            Step::Observe(14, Some(5_300), Quiet),
            Step::Observe(15, Some(5_400), Send),
        ]);
    }

    #[test]
    fn flat_lag_requires_actual_growth_even_after_duration() {
        use AlertDecision::{None as Quiet, Send};
        check(&[
            Step::Observe(0, Some(5_100), Quiet),
            Step::Observe(5, Some(5_100), Quiet),
            Step::Observe(10, Some(5_100), Quiet),
            Step::Observe(60, Some(5_100), Quiet),
            Step::Observe(61, Some(5_101), Send),
        ]);
    }

    #[test]
    fn plateau_preserves_actual_growth_and_duration() {
        use AlertDecision::{None as Quiet, Send};
        check(&[
            Step::Observe(0, Some(5_100), Quiet),
            Step::Observe(5, Some(5_200), Quiet),
            Step::Observe(9, Some(5_200), Quiet),
            Step::Observe(10, Some(5_200), Send),
        ]);
    }

    #[test]
    fn decrease_restarts_window_from_the_current_sample() {
        use AlertDecision::{None as Quiet, Send};
        check(&[
            Step::Observe(0, Some(5_100), Quiet),
            Step::Observe(5, Some(5_300), Quiet),
            Step::Observe(9, Some(5_200), Quiet),
            Step::Observe(10, Some(5_250), Quiet),
            Step::Observe(18, Some(5_300), Quiet),
            Step::Observe(19, Some(5_300), Send),
        ]);
    }

    #[test]
    fn decrease_clears_growth_evidence() {
        use AlertDecision::None as Quiet;
        check(&[
            Step::Observe(0, Some(5_100), Quiet),
            Step::Observe(5, Some(5_300), Quiet),
            Step::Observe(9, Some(5_200), Quiet),
            Step::Observe(19, Some(5_200), Quiet),
            Step::Observe(60, Some(5_200), Quiet),
        ]);
    }

    #[test]
    fn invalid_total_lag_immediately_clears_candidate() {
        use AlertDecision::{None as Quiet, Send};
        check(&[
            Step::Observe(0, Some(5_100), Quiet),
            Step::Observe(5, Some(5_200), Quiet),
            Step::Observe(9, None, Quiet),
            Step::Observe(10, Some(5_300), Quiet),
            Step::Observe(19, Some(5_400), Quiet),
            Step::Observe(20, Some(5_500), Send),
        ]);
    }

    #[test]
    fn threshold_or_below_clears_candidate() {
        use AlertDecision::{None as Quiet, Send};
        for recovered_lag in [5_000, 4_999, 0] {
            check(&[
                Step::Observe(0, Some(5_100), Quiet),
                Step::Observe(5, Some(5_200), Quiet),
                Step::Observe(9, Some(recovered_lag), Quiet),
                Step::Observe(10, Some(5_300), Quiet),
                Step::Observe(19, Some(5_400), Quiet),
                Step::Observe(20, Some(5_500), Send),
            ]);
        }
    }

    #[test]
    fn successful_notification_is_sent_once_until_valid_recovery() {
        use AlertDecision::{None as Quiet, Send};
        for recovered_lag in [5_000, 4_999] {
            check(&[
                Step::Observe(0, Some(5_100), Quiet),
                Step::Observe(10, Some(5_200), Send),
                Step::Attempt(11, true),
                Step::Observe(20, Some(5_300), Quiet),
                Step::Observe(21, None, Quiet),
                Step::Observe(30, Some(5_100), Quiet),
                Step::Observe(40, Some(5_200), Quiet),
                Step::Observe(41, Some(5_100), Quiet),
                Step::Observe(51, Some(5_300), Quiet),
                Step::Observe(52, Some(recovered_lag), Quiet),
                Step::Observe(53, Some(5_100), Quiet),
                Step::Observe(63, Some(5_200), Send),
            ]);
        }
    }

    #[test]
    fn failed_attempt_retries_thirty_seconds_after_result_then_stops_on_success() {
        use AlertDecision::{None as Quiet, Send};
        check(&[
            Step::Observe(0, Some(5_100), Quiet),
            Step::Observe(10, Some(5_200), Send),
            Step::Attempt(12, false),
            Step::Observe(40, Some(5_300), Quiet),
            Step::Observe(41, Some(5_400), Quiet),
            Step::Observe(42, Some(5_500), Send),
            Step::Attempt(43, true),
            Step::Observe(73, Some(5_600), Quiet),
        ]);
    }

    #[test]
    fn third_failure_suppresses_until_valid_recovery() {
        use AlertDecision::{None as Quiet, Send};
        for recovered_lag in [5_000, 4_999] {
            check(&[
                Step::Observe(0, Some(5_100), Quiet),
                Step::Observe(10, Some(5_200), Send),
                Step::Attempt(10, false),
                Step::Observe(39, Some(5_300), Quiet),
                Step::Observe(40, Some(5_400), Send),
                Step::Attempt(40, false),
                Step::Observe(69, Some(5_500), Quiet),
                Step::Observe(70, Some(5_600), Send),
                Step::Attempt(70, false),
                Step::Observe(100, Some(5_700), Quiet),
                Step::Observe(101, None, Quiet),
                Step::Observe(102, Some(5_100), Quiet),
                Step::Observe(112, Some(5_200), Quiet),
                Step::Observe(113, Some(5_100), Quiet),
                Step::Observe(123, Some(5_300), Quiet),
                Step::Observe(124, Some(recovered_lag), Quiet),
                Step::Observe(125, Some(5_100), Quiet),
                Step::Observe(135, Some(5_200), Send),
                Step::Attempt(135, false),
                Step::Observe(165, Some(5_300), Send),
            ]);
        }
    }

    #[test]
    fn invalid_or_decreased_lag_requires_a_new_growth_window_before_retry() {
        use AlertDecision::{None as Quiet, Send};
        for interrupted_lag in [None, Some(5_050)] {
            check(&[
                Step::Observe(0, Some(5_100), Quiet),
                Step::Observe(10, Some(5_200), Send),
                Step::Attempt(10, false),
                Step::Observe(39, interrupted_lag, Quiet),
                Step::Observe(40, Some(5_100), Quiet),
                Step::Observe(48, Some(5_200), Quiet),
                Step::Observe(50, Some(5_300), Send),
                Step::Attempt(50, false),
                Step::Observe(80, Some(5_400), Send),
                Step::Attempt(80, false),
                Step::Observe(110, Some(5_500), Quiet),
            ]);
        }
    }

    #[test]
    fn observe_does_not_issue_another_send_while_attempt_is_in_flight() {
        use AlertDecision::{None as Quiet, Send};
        check(&[
            Step::Observe(0, Some(5_100), Quiet),
            Step::Observe(10, Some(5_200), Send),
            Step::Observe(20, Some(5_300), Quiet),
            Step::Observe(40, Some(5_400), Quiet),
            Step::Attempt(41, false),
            Step::Observe(70, Some(5_500), Quiet),
            Step::Observe(71, Some(5_600), Send),
        ]);
    }

    #[test]
    fn recovery_cancels_waiting_retry() {
        use AlertDecision::{None as Quiet, Send};
        check(&[
            Step::Observe(0, Some(5_100), Quiet),
            Step::Observe(10, Some(5_200), Send),
            Step::Attempt(10, false),
            Step::Observe(11, Some(5_000), Quiet),
            Step::Observe(12, Some(5_100), Quiet),
            Step::Observe(22, Some(5_200), Send),
        ]);
    }
}
