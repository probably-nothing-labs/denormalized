use std::any::Any;
use std::sync::Arc;
use std::time::Duration;

use datafusion::common::cast::as_float64_array;
use datafusion::functions_aggregate::average::avg;
use datafusion::functions_aggregate::count::count;
use datafusion::functions_aggregate::expr_fn::{max, min};
use datafusion::logical_expr::sort_properties::{ExprProperties, SortProperties};
use datafusion::logical_expr::Volatility;
use datafusion::logical_expr::{col, lit};
use datafusion::logical_expr::{ColumnarValue, ScalarUDF, ScalarUDFImpl, Signature};

use denormalized::datasource::kafka::{ConnectionOpts, KafkaTopicBuilder};
use denormalized::prelude::*;

use arrow::array::{ArrayRef, Float64Array};
use arrow::datatypes::DataType;

use denormalized_examples::get_sample_json;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .init();

    let sample_event = get_sample_json();

    let bootstrap_servers = String::from("localhost:9092");

    let ctx = Context::new()?;
    let mut topic_builder = KafkaTopicBuilder::new(bootstrap_servers.clone());

    let source_topic = topic_builder
        .with_topic(String::from("temperature"))
        .infer_schema_from_json(sample_event.as_str())?
        .with_encoding("json")?
        .with_timestamp(String::from("occurred_at_ms"), TimestampUnit::Int64Millis)
        .build_reader(ConnectionOpts::from([
            ("auto.offset.reset".to_string(), "latest".to_string()),
            ("group.id".to_string(), "sample_pipeline".to_string()),
        ]))
        .await?;

    let sample_udf = ScalarUDF::from(SampleUdf::new());

    let ds = ctx
        .from_topic(source_topic)
        .await?
        .window(
            vec![col("sensor_name")],
            vec![
                count(col("reading")).alias("count"),
                min(col("reading")).alias("min"),
                max(col("reading")).alias("max"),
                avg(col("reading")).alias("average"),
            ],
            Duration::from_millis(1_000),
            None,
        )?
        .filter(col("max").gt(lit(113)))?
        .with_column("sample", sample_udf.call(vec![col("max")]))?;

    ds.clone().print_physical_plan().await?;
    ds.clone().print_stream().await?;

    Ok(())
}

#[derive(Debug, Clone)]
struct SampleUdf {
    signature: Signature,
}

impl SampleUdf {
    fn new() -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Float64], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SampleUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "sample_udf"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> datafusion::error::Result<ColumnarValue> {
        assert_eq!(args.len(), 1);
        let value = &args[0];
        assert_eq!(value.data_type(), DataType::Float64);

        let args = ColumnarValue::values_to_arrays(args)?;
        let value = as_float64_array(&args[0]).expect("cast failed");

        let array = value
            .iter()
            .map(|v| match v {
                Some(f) => {
                    let value = f + 20_f64;
                    Some(value)
                }
                _ => None,
            })
            .collect::<Float64Array>();

        Ok(ColumnarValue::from(Arc::new(array) as ArrayRef))
    }

    fn output_ordering(
        &self,
        input: &[ExprProperties],
    ) -> datafusion::error::Result<SortProperties> {
        Ok(input[0].sort_properties)
    }
}
