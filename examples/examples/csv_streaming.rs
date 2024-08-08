use datafusion::common::test_util::datafusion_test_data;
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::datasource::MemTable;
use datafusion::error::Result;
use datafusion::prelude::*;
use datafusion::logical_expr::{col, max, min};

/// This example demonstrates executing a simple query against an Arrow data source (CSV) and
/// fetching results with streaming aggregation and streaming window
#[tokio::main]
async fn main() -> Result<()> {
    // create local execution context
    let ctx = SessionContext::new();

    // Register a csv table
    let testdata = datafusion_test_data();
    ctx.register_csv(
        "ordered_table",
        &format!("{testdata}/window_1.csv"),
        CsvReadOptions::new().file_sort_order(vec![vec![col("ts").sort(true, true)]]),
    )
    .await?;

    // Aggregate teh csv data
    let df = ctx
        .clone()
        .table("ordered_table")
        .await?
        .aggregate(
            vec![col("ts")],
            vec![
                min(col("inc_col")).alias("min"),
                max(col("inc_col")).alias("max"),
            ],
        )?
        .sort(vec![col("ts").sort(true, true)])?;

    // create and register a new memtable with the same schema as the aggregated results
    let csv_schema = datafusion::common::arrow::datatypes::Schema::from(df.schema());

    let mem_table = MemTable::try_new(std::sync::Arc::new(csv_schema.clone()), vec![vec![]])?;
    ctx.register_table("out_table", std::sync::Arc::new(mem_table))?;

    // read, aggregrate, then write the csv file into a memtable
    let out = df
        .clone()
        .write_table("out_table", DataFrameWriteOptions::default())
        .await?;

    println!("{:?}", out);

    // Write the results of the memtable to a csv file on disk
    let res = ctx
        .table("out_table")
        .await?
        .write_csv("out.csv", DataFrameWriteOptions::new(), None)
        .await?;

    println!("Data written to csv {:?}", res);

    Ok(())
}
