/// The latest retained offset; the high watermark is the next offset to write.
// 在生成分区快照时将下一写入位置 High 换算为最大保留消息 Offset，空范围返回 None 表示没有最大消息 Offset。
pub fn latest_message_offset(low: i64, high: i64) -> Option<i64> {
    (high > low).then(|| high - 1)
}

/// Offset span, not a count of messages (compaction may leave gaps).
// 在生成水位快照时计算非负 Offset 范围估算；压缩可能留空洞，因此不能作为精确消息数。
pub fn retained_offset_span(low: i64, high: i64) -> u64 {
    high.saturating_sub(low).max(0) as u64
}

/// Uncommitted partitions have unknown lag rather than zero lag.
// 在生成消费分区快照时计算非负 Lag，未提交位置通过 None 传播未知而不是零。
pub fn lag(high: i64, committed: Option<i64>) -> Option<u64> {
    committed.map(|offset| high.saturating_sub(offset).max(0) as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    // 在领域回归中验证 High 的排他语义及 Lag 计算，防止 Offset 差一。
    fn derives_partition_metrics_without_off_by_one() {
        assert_eq!(latest_message_offset(120, 98_241), Some(98_240));
        assert_eq!(retained_offset_span(120, 98_241), 98_121);
        assert_eq!(lag(98_241, Some(90_031)), Some(8_210));
    }

    #[test]
    // 在领域回归中验证空分区与未提交状态显式保留，防止未知值降为零。
    fn empty_and_uncommitted_are_explicit() {
        assert_eq!(latest_message_offset(5, 5), None);
        assert_eq!(retained_offset_span(5, 5), 0);
        assert_eq!(lag(5, None), None);
    }

    #[test]
    // 在领域回归中验证倒置水位和超前提交的负差值被钳制为零。
    fn negative_differences_are_clamped_to_zero() {
        assert_eq!(retained_offset_span(10, 5), 0);
        assert_eq!(latest_message_offset(10, 5), None);
        assert_eq!(lag(5, Some(10)), Some(0));
    }

    #[test]
    // 在领域回归中验证极小水位的空分区不会发生减一溢出。
    fn empty_extreme_watermark_does_not_underflow() {
        assert_eq!(latest_message_offset(i64::MIN, i64::MIN), None);
    }
}
