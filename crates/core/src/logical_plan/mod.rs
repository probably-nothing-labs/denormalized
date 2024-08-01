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

use datafusion_common::Result;

use datafusion_expr::builder::add_group_by_exprs_from_dependencies;
use datafusion_expr::expr_rewriter::normalize_cols;
use datafusion_expr::logical_plan::{Extension, LogicalPlan};
use datafusion_expr::LogicalPlanBuilder;
use datafusion_expr::{Aggregate, Expr};

pub mod streaming_window;
use streaming_window::{StreamingWindowPlanNode, StreamingWindowSchema, StreamingWindowType};

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

        let plan = self.plan.clone();

        Aggregate::try_new(Arc::new(self.plan), group_expr, aggr_expr)
            .map(|new_aggr| {
                LogicalPlan::Extension(Extension {
                    node: Arc::new(StreamingWindowPlanNode {
                        window_type: window,
                        window_schema: StreamingWindowSchema::try_new(new_aggr.clone()).unwrap(),
                        aggregrate: new_aggr.clone(),
                        input: plan,
                    }),
                })
            })
            .map(Self::from)
    }
}
