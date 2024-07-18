use datafusion::common::test_util::datafusion_test_data;
use datafusion::datasource::provider_as_source;
use datafusion::error::Result;
use datafusion::prelude::*;
use datafusion_expr::{col, max, min, LogicalPlanBuilder};

/// This example demonstrates executing a simple query against an Arrow data source (CSV) and
/// fetching results with streaming aggregation and streaming window
#[tokio::main]
async fn main() -> Result<()> {
    // create local execution context
    let ctx = SessionContext::new();

    let testdata = datafusion_test_data();

    // Register a table source and tell DataFusion the file is ordered by `ts ASC`.
    // Note it is the responsibility of the user to make sure
    // that file indeed satisfies this condition or else incorrect answers may be produced.
    let asc = true;
    let nulls_first = true;
    let sort_expr = vec![col("ts").sort(asc, nulls_first)];
    // register csv file with the execution context
    ctx.register_csv(
        "ordered_table",
        &format!("{testdata}/window_1.csv"),
        CsvReadOptions::new().file_sort_order(vec![sort_expr]),
    )
    .await?;

    let table_provider = ctx.table_provider("ordered_table").await?;

    //
    // Example of using the raw LogicalPlanBuilder interface
    //
    let plan = LogicalPlanBuilder::scan(
        "ordered_table",
        provider_as_source(table_provider.clone()),
        None,
    )?
    .aggregate(
        vec![col("ts")],
        vec![
            min(col("inc_col")).alias("min"),
            max(col("inc_col")).alias("max"),
        ],
    )?
    .sort(vec![Expr::Sort(datafusion::logical_expr::SortExpr::new(
        Box::new(col("ts")),
        true,
        true,
    ))])?
    .build()?;
    println!("LogicalPlan result");
    DataFrame::new(ctx.state(), plan).show().await?;

    //
    // Example of using the SQL interface
    //
    let df = ctx
        .clone()
        .sql(
            "SELECT ts, MIN(inc_col) as min, MAX(inc_col) as max \
        FROM ordered_table \
        GROUP BY ts ORDER BY ts",
        )
        .await?;
    println!("SQL result");
    df.show().await?;

    //
    // Example of using the DataFrame interface
    //
    let df = ctx
        .clone()
        .read_table(table_provider.clone())?
        .aggregate(
            vec![col("ts")],
            vec![
                min(col("inc_col")).alias("min"),
                max(col("inc_col")).alias("max"),
            ],
        )?
        .sort(vec![Expr::Sort(datafusion::logical_expr::SortExpr::new(
            Box::new(col("ts")),
            true,
            true,
        ))])?;
    println!("DataFrame result");
    df.show().await?;

    Ok(())
}
