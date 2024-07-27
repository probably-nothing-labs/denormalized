use core::fmt::Debug;

use std::sync::Arc;
use std::time::Duration;
use std::fmt;

use arrow::datatypes::{DataType, Field, SchemaBuilder, TimeUnit};

use datafusion_common::{DFSchema, DFSchemaRef, Result};
use datafusion_expr::{Aggregate, Expr};
use datafusion::logical_expr::{
    LogicalPlan, UserDefinedLogicalNode, UserDefinedLogicalNodeCore,
};

#[derive(PartialEq, Eq, Hash)]
pub struct StreamingWindowPlanNode {
    pub window_type: StreamingWindowType,
    pub window_schema: StreamingWindowSchema,
    pub agg: Aggregate,

    pub input: LogicalPlan,
    /// The sort expression (this example only supports a single sort
    /// expr)
    pub expr: Expr,

}

impl Debug for StreamingWindowPlanNode {
    /// For TopK, use explain format for the Debug format. Other types
    /// of nodes may
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
        // @todo should be the output schema
        self.input.schema()
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![self.expr.clone()]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "StreamingWindow @todo")
    }

    fn with_exprs_and_inputs(
        &self,
        mut exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        Ok(Self {
            window_type: self.window_type,
            window_schema: self.window_schema,
            agg: self.agg,
            input: inputs.swap_remove(0),
            expr: exprs.swap_remove(0),
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
        let fields = inner_schema.all_fields().to_owned();

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
