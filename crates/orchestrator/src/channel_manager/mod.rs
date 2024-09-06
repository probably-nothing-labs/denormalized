use crossbeam::channel;
use log::debug;
use once_cell::sync::Lazy;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

use crate::orchestrator::OrchestrationMessage;

struct ChannelPair {
    sender: channel::Sender<OrchestrationMessage>,
    receiver: Option<channel::Receiver<OrchestrationMessage>>,
}

struct Channels {
    channels: HashMap<String, ChannelPair>,
}

static GLOBAL_CHANNELS: Lazy<Arc<RwLock<Channels>>> = Lazy::new(|| {
    Arc::new(RwLock::new(Channels {
        channels: HashMap::new(),
    }))
});

// Function to create a new channel
pub fn create_channel(id: &str, buffer: usize) {
    debug!("create request for channel {} with size {}", id, buffer);
    let mut channels = GLOBAL_CHANNELS.write();
    let (tx, rx) = channel::unbounded();

    channels.channels.insert(
        id.to_string(),
        ChannelPair {
            sender: tx,
            receiver: Some(rx),
        },
    );
}

pub fn get_sender(id: &str) -> Option<channel::Sender<OrchestrationMessage>> {
    let channels = GLOBAL_CHANNELS.read();
    channels.channels.get(id).map(|pair| pair.sender.clone())
}

pub fn take_receiver(id: &str) -> Option<channel::Receiver<OrchestrationMessage>> {
    let mut channels = GLOBAL_CHANNELS.write();
    channels
        .channels
        .get_mut(id)
        .and_then(|pair| pair.receiver.take())
}
