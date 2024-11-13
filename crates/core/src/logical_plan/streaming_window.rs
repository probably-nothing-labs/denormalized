use core::fmt::Debug;

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use arrow::datatypes::{DataType, Field, SchemaBuilder, TimeUnit};

use datafusion::common::{DFSchema, DFSchemaRef, Result};
use datafusion::logical_expr::{Aggregate, Expr};
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNodeCore};

//TODO: Avoid use of Aggregate here as we need to clone the internal expressions back and forth.
#[derive(PartialEq, Eq, Hash)]
pub struct StreamingWindowPlanNode {
    pub window_type: StreamingWindowType,
    pub window_schema: StreamingWindowSchema,
    pub aggregrate: Aggregate,
    pub input: LogicalPlan,
}

impl Debug for StreamingWindowPlanNode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        UserDefinedLogicalNodeCore::fmt_for_explain(self, f)
    }
}

impl UserDefinedLogicalNodeCore for StreamingWindowPlanNode {
    fn name(&self) -> &str {
        "StreamingWindow"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.window_schema.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        self.aggregrate.aggr_expr.clone()
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "StreamingWindow @todo")
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        let input = inputs.swap_remove(0);
        let new_aggregation = Aggregate::try_new(
            Arc::new(input.clone()),
            self.aggregrate.group_expr.clone(),
            exprs,
        )?;
        Ok(Self {
            window_type: self.window_type.clone(),
            window_schema: self.window_schema.clone(),
            aggregrate: new_aggregation,
            input,
        })
    }
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum StreamingWindowType {
    Tumbling(Duration),
    Sliding(Duration, Duration),
    Session(Duration, String),
}

#[derive(Clone, PartialEq, Eq, Hash)]
// mark non_exhaustive to encourage use of try_new/new()
#[non_exhaustive]
pub struct StreamingWindowSchema {
    pub schema: DFSchemaRef,
}

impl StreamingWindowSchema {
    pub fn try_new(aggr_expr: Aggregate) -> Result<Self> {
        let inner_schema = aggr_expr.schema.inner().clone();
        let fields = inner_schema.fields();

        let mut builder = SchemaBuilder::new();

        for field in fields {
            builder.push(field.clone());
        }
        builder.push(Field::new(
            "window_start_time",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ));
        builder.push(Field::new(
            "window_end_time",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ));
        let schema_with_window_columns = DFSchema::try_from(builder.finish())?;
        Ok(StreamingWindowSchema {
            schema: Arc::new(schema_with_window_columns),
        })
    }
}
