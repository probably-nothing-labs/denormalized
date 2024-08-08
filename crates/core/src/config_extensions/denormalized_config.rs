use datafusion::common::extensions_options;
use datafusion::config::ConfigExtension;

extensions_options! {
    pub struct DenormalizedConfig {
        pub checkpoint: bool, default = false
    }
}

impl ConfigExtension for DenormalizedConfig {
    const PREFIX: &'static str = "denormalized_config";
}
