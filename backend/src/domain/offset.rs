/// The latest retained offset; the high watermark is the next offset to write.
pub fn latest_message_offset(low: i64, high: i64) -> Option<i64> {
    (high > low).then(|| high - 1)
}

/// Offset span, not a count of messages (compaction may leave gaps).
pub fn retained_offset_span(low: i64, high: i64) -> u64 {
    high.saturating_sub(low).max(0) as u64
}

/// Uncommitted partitions have unknown lag rather than zero lag.
pub fn lag(high: i64, committed: Option<i64>) -> Option<u64> {
    committed.map(|offset| high.saturating_sub(offset).max(0) as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn derives_partition_metrics_without_off_by_one() {
        assert_eq!(latest_message_offset(120, 98_241), Some(98_240));
        assert_eq!(retained_offset_span(120, 98_241), 98_121);
        assert_eq!(lag(98_241, Some(90_031)), Some(8_210));
    }

    #[test]
    fn empty_and_uncommitted_are_explicit() {
        assert_eq!(latest_message_offset(5, 5), None);
        assert_eq!(retained_offset_span(5, 5), 0);
        assert_eq!(lag(5, None), None);
    }

    #[test]
    fn negative_differences_are_clamped_to_zero() {
        assert_eq!(retained_offset_span(10, 5), 0);
        assert_eq!(latest_message_offset(10, 5), None);
        assert_eq!(lag(5, Some(10)), Some(0));
    }

    #[test]
    fn empty_extreme_watermark_does_not_underflow() {
        assert_eq!(latest_message_offset(i64::MIN, i64::MIN), None);
    }
}
