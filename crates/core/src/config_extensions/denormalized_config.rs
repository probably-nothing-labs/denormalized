use datafusion::config::ConfigExtension;
use datafusion::common::extensions_options;

extensions_options! {
    pub struct DenormalizedConfig {
        pub checkpoint: bool, default = false
    }
}

impl ConfigExtension for DenormalizedConfig {
    const PREFIX: &'static str = "denormalized_config";
}
