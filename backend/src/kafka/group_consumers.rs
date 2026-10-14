use super::{collector::ProbeError, config::build_client_properties};
use crate::{domain::cluster::DecryptedClusterConfig, sessions::manager::SessionContext};
use rdkafka::consumer::BaseConsumer;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

/// Session-owned readers: never assigned, subscribed, polled, or committed.
#[derive(Clone, Default)]
pub struct GroupConsumerCache(Arc<Mutex<HashMap<String, Arc<BaseConsumer<SessionContext>>>>>);

impl GroupConsumerCache {
    pub fn get_or_create(
        &self,
        group: &str,
        config: &DecryptedClusterConfig,
    ) -> Result<Arc<BaseConsumer<SessionContext>>, ProbeError> {
        let mut cache = self.0.lock().map_err(|_| ProbeError::Query)?;
        if let Some(consumer) = cache.get(group) {
            return Ok(consumer.clone());
        }
        let mut properties =
            build_client_properties(config).map_err(|_| ProbeError::InvalidConfig)?;
        properties
            .inner
            .set("group.id", group)
            .set("enable.auto.commit", "false")
            .set("enable.auto.offset.store", "false");
        let consumer: Arc<BaseConsumer<SessionContext>> = Arc::new(
            properties
                .inner
                .create_with_context(SessionContext)
                .map_err(|_| ProbeError::InvalidConfig)?,
        );
        cache.insert(group.into(), consumer.clone());
        Ok(consumer)
    }
}
