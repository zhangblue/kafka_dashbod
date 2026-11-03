//! Kafka 配置、临时探测、只读采集和受控 Topic 管理；不提供消息写入或偏移修改。

pub mod client;
pub mod collector;
pub mod config;
pub mod group_consumers;
pub mod topic_admin;
