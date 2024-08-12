use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct Measurment {
    pub occurred_at_ms: u64,
    pub sensor_name: String,
    pub reading: f64,
}

pub fn get_sample_json() -> String {
    serde_json::to_string(&Measurment {
        occurred_at_ms: 100,
        sensor_name: "foo".to_string(),
        reading: 0.,
    })
    .unwrap()
}
