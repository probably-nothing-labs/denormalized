use denormalized_channels::channel_manager::{create_channel, take_receiver};
use log::{debug, info};

pub struct Orchestrator {}

impl Orchestrator {
    pub fn run(&self, managed_tasks: usize) {
        info!("creating a channel");
        create_channel("orchestrator", managed_tasks);
        let receiver = take_receiver("orchestrator");
        info!("receiver is {:?}", receiver);
        loop {
            let msg = receiver.as_ref().unwrap().recv().unwrap();
            info!("Received from blocking channel: {}", msg);
        }
    }
}
