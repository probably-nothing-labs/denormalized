use std::{
    collections::HashMap,
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use crate::channel_manager::{create_channel, get_sender, take_receiver};
use crossbeam::channel;
use log::debug;
use tokio::sync::watch;

#[derive(Clone, Debug)]
pub enum OrchestrationMessage {
    RegisterStream(String),
    CheckpointBarrier(u128),
    CheckpointComplete(String),
}

#[derive(Default)]
pub struct Orchestrator {
    senders: HashMap<String, channel::Sender<OrchestrationMessage>>,
}

/**
 * 1. Keep track of checkpoint per source.
 * 2. Tell each downstream which checkpoints it needs to know.
 * 3. Send a checkpoint request every time you have a new barrier
 * 4. Figure out if the checkpoint was complete.
 */
impl Orchestrator {
    pub fn run(&mut self, managed_tasks: usize, shutdown_rx: watch::Receiver<bool>) {
        debug!("Orchestrator started.");
        create_channel("orchestrator", managed_tasks); // Currently we are going to use unbounded channels
        let receiver = take_receiver("orchestrator");
        let mut last_checkpoint = Instant::now();
        loop {
            // Check if shutdown signal has been received
            if shutdown_rx.has_changed().unwrap_or(false) && *shutdown_rx.borrow() {
                debug!("Shutdown signal received. Exiting orchestrator...");
                break;
            }

            if !receiver.as_ref().unwrap().is_empty() {
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

            let time_now = Instant::now();
            let diff = time_now - last_checkpoint;

            if diff.as_millis() >= 10_000 {
                let epoch_ts = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis();

                for (stream_id, sender) in self.senders.iter() {
                    match sender.try_send(OrchestrationMessage::CheckpointBarrier(epoch_ts)) {
                        Ok(_) => continue,
                        Err(_) => log::error!(
                            "Error in sending checkpoint barrier to stream {}",
                            stream_id
                        ),
                    }
                }
                last_checkpoint = time_now;
                debug!(
                    "completed sending checkpoint barrier for {:?}",
                    last_checkpoint
                )
            }
        }
    }
}
