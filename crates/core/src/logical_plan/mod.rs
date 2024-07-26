// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::sync::Arc;
use std::time::Duration;

use datafusion_common::{DFSchema, DFSchemaRef, Result};

use datafusion_expr::builder::add_group_by_exprs_from_dependencies;
use datafusion_expr::expr_rewriter::normalize_cols;
use datafusion_expr::logical_plan::LogicalPlan;
use datafusion_expr::LogicalPlanBuilder;
use datafusion_expr::{Aggregate, Expr};

use arrow::datatypes::{DataType, Field, Schema, SchemaBuilder, SchemaRef, TimeUnit};

pub trait StreamingLogicalPlanBuilder {
    fn streaming_window(
        self,
        group_expr: impl IntoIterator<Item = impl Into<Expr>>,
        aggr_expr: impl IntoIterator<Item = impl Into<Expr>>,
        window_length: Duration,
        slide: Option<Duration>,
    ) -> Result<LogicalPlanBuilder>;
}

// Extend the LogicalPlanBuilder with functions to add streaming operators to the plan
impl StreamingLogicalPlanBuilder for LogicalPlanBuilder {
    /// Apply franz window functions to extend the schema
    fn streaming_window(
        self,
        group_expr: impl IntoIterator<Item = impl Into<Expr>>,
        aggr_expr: impl IntoIterator<Item = impl Into<Expr>>,
        window_length: Duration,
        slide: Option<Duration>,
    ) -> Result<Self> {
        let group_expr = normalize_cols(group_expr, &self.plan)?;
        let aggr_expr = normalize_cols(aggr_expr, &self.plan)?;

        let group_expr = add_group_by_exprs_from_dependencies(group_expr, self.plan.schema())?;
        let window: StreamingWindowType = slide.map_or_else(
            || StreamingWindowType::Tumbling(window_length),
            |_slide| StreamingWindowType::Sliding(window_length, _slide),
        );
        let aggr = Aggregate::try_new(Arc::new(self.plan), group_expr, aggr_expr)
            .map(|new_aggr| {
                LogicalPlan::StreamingWindow(
                    new_aggr.clone(),
                    window,
                    StreamingWindowSchema::try_new(new_aggr).unwrap(),
                )
            })
            .map(Self::from);
        aggr
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
