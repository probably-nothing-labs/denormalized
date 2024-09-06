use std::collections::HashMap;

use crate::channel_manager::{create_channel, get_sender, take_receiver};
use crossbeam::channel;
use log::{debug, info};

#[derive(Clone, Debug)]
pub enum OrchestrationMessage {
    RegisterStream(String),
    CheckpointBarrier(String),
    CheckpointComplete(String),
}

pub struct Orchestrator {
    senders: HashMap<String, channel::Sender<OrchestrationMessage>>,
}

pub const SHOULD_CHECKPOINT: bool = false; // THIS WILL BE MOVED INTO CONFIG

impl Orchestrator {
    pub fn default() -> Self {
        Self {
            senders: HashMap::new(),
        }
    }

    pub fn run(&mut self, managed_tasks: usize) {
        info!("Orchestrator started.");
        create_channel("orchestrator", managed_tasks);
        let receiver = take_receiver("orchestrator");
        loop {
            let msg: OrchestrationMessage = receiver.as_ref().unwrap().recv().unwrap();
            match msg {
                OrchestrationMessage::RegisterStream(stream_id) => {
                    debug!("registering stream {}", stream_id);
                    let sender = get_sender(&stream_id).unwrap();
                    self.senders.insert(stream_id, sender);
                }
                OrchestrationMessage::CheckpointBarrier(_) => todo!(),
                OrchestrationMessage::CheckpointComplete(_) => todo!(),
            }
        }
    }
}
