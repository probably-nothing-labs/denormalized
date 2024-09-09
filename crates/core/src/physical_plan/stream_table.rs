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

//! Generic plans for deferred execution: [`StreamingTableExec`] and [`PartitionStream`]

use std::any::Any;
use std::sync::Arc;

//use super::{DisplayAs, DisplayFormatType, ExecutionMode, PlanProperties};
//use crate::display::{display_orderings, ProjectSchemaDisplay};
//use crate::stream::RecordBatchStreamAdapter;
//use crate::{ExecutionPlan, Partitioning, SendableRecordBatchStream};
use arrow::datatypes::SchemaRef;
use arrow_schema::Schema;
use datafusion::{
    common::{internal_err, plan_err, Result},
    physical_expr::EquivalenceProperties,
    physical_plan::{
        display::{display_orderings, ProjectSchemaDisplay},
        limit::LimitStream,
        metrics::{BaselineMetrics, MetricsSet},
        stream::RecordBatchStreamAdapter,
        streaming::PartitionStream,
        DisplayAs, DisplayFormatType, ExecutionMode, ExecutionPlan, Partitioning,
    },
};
//use datafusion_execution::TaskContext;
//use datafusion_physical_expr::{EquivalenceProperties, LexOrdering};

//use crate::limit::LimitStream;
//use crate::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use async_trait::async_trait;
use datafusion::{
    execution::{SendableRecordBatchStream, TaskContext},
    physical_expr::LexOrdering,
    physical_plan::{metrics::ExecutionPlanMetricsSet, PlanProperties},
};
use futures::stream::StreamExt;
use log::debug;

use crate::datasource::kafka::KafkaStreamRead;

pub trait PartitionStreamExt: PartitionStream {
    fn requires_node_id(&self) -> bool {
        false
    }

    fn as_partition_with_node_id(&self) -> Option<&KafkaStreamRead> {
        None
    }
}

/// An [`ExecutionPlan`] for one or more [`PartitionStream`]s.
///
/// If your source can be represented as one or more [`PartitionStream`]s, you can
/// use this struct to implement [`ExecutionPlan`].
pub struct DenormalizedStreamingTableExec {
    partitions: Vec<Arc<dyn PartitionStreamExt>>,
    projection: Option<Arc<[usize]>>,
    projected_schema: SchemaRef,
    projected_output_ordering: Vec<LexOrdering>,
    infinite: bool,
    limit: Option<usize>,
    cache: PlanProperties,
    metrics: ExecutionPlanMetricsSet,
}

impl DenormalizedStreamingTableExec {
    /// Try to create a new [`StreamingTableExec`] returning an error if the schema is incorrect
    pub fn try_new(
        schema: SchemaRef,
        partitions: Vec<Arc<dyn PartitionStreamExt>>,
        projection: Option<&Vec<usize>>,
        projected_output_ordering: impl IntoIterator<Item = LexOrdering>,
        infinite: bool,
        limit: Option<usize>,
    ) -> Result<Self> {
        for x in partitions.iter() {
            let partition_schema = x.schema();
            if !schema.eq(partition_schema) {
                debug!(
                    "Target schema does not match with partition schema. \
                        Target_schema: {schema:?}. Partition Schema: {partition_schema:?}"
                );
                return plan_err!("Mismatch between schema and batches");
            }
        }

        let projected_schema = match projection {
            Some(p) => Arc::new(schema.project(p)?),
            None => schema,
        };
        let projected_output_ordering = projected_output_ordering.into_iter().collect::<Vec<_>>();
        let cache = Self::compute_properties(
            Arc::clone(&projected_schema),
            &projected_output_ordering,
            &partitions,
            infinite,
        );
        Ok(Self {
            partitions,
            projected_schema,
            projection: projection.cloned().map(Into::into),
            projected_output_ordering,
            infinite,
            limit,
            cache,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }

    pub fn partitions(&self) -> &Vec<Arc<dyn PartitionStreamExt>> {
        &self.partitions
    }

    pub fn partition_schema(&self) -> &SchemaRef {
        self.partitions[0].schema()
    }

    pub fn projection(&self) -> &Option<Arc<[usize]>> {
        &self.projection
    }

    pub fn projected_schema(&self) -> &Schema {
        &self.projected_schema
    }

    pub fn projected_output_ordering(&self) -> impl IntoIterator<Item = LexOrdering> {
        self.projected_output_ordering.clone()
    }

    pub fn is_infinite(&self) -> bool {
        self.infinite
    }

    pub fn limit(&self) -> Option<usize> {
        self.limit
    }

    /// This function creates the cache object that stores the plan properties such as schema, equivalence properties, ordering, partitioning, etc.
    fn compute_properties(
        schema: SchemaRef,
        orderings: &[LexOrdering],
        partitions: &[Arc<dyn PartitionStreamExt>],
        is_infinite: bool,
    ) -> PlanProperties {
        // Calculate equivalence properties:
        let eq_properties = EquivalenceProperties::new_with_orderings(schema, orderings);

        // Get output partitioning:
        let output_partitioning = Partitioning::UnknownPartitioning(partitions.len());

        // Determine execution mode:
        let mode = if is_infinite {
            ExecutionMode::Unbounded
        } else {
            ExecutionMode::Bounded
        };

        PlanProperties::new(eq_properties, output_partitioning, mode)
    }
}

impl std::fmt::Debug for DenormalizedStreamingTableExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LazyMemTableExec").finish_non_exhaustive()
    }
}

impl DisplayAs for DenormalizedStreamingTableExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "DenormalizedStreamingTableExec: partition_sizes={:?}",
                    self.partitions.len(),
                )?;
                if !self.projected_schema.fields().is_empty() {
                    write!(
                        f,
                        ", projection={}",
                        ProjectSchemaDisplay(&self.projected_schema)
                    )?;
                }
                if self.infinite {
                    write!(f, ", infinite_source=true")?;
                }
                if let Some(fetch) = self.limit {
                    write!(f, ", fetch={fetch}")?;
                }

                display_orderings(f, &self.projected_output_ordering)?;

                Ok(())
            }
        }
    }
}

#[async_trait]
impl ExecutionPlan for DenormalizedStreamingTableExec {
    fn name(&self) -> &'static str {
        "DenormalizedStreamingTableExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn fetch(&self) -> Option<usize> {
        self.limit
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(self)
        } else {
            internal_err!("Children cannot be replaced in {self:?}")
        }
    }

    fn execute(
        &self,
        partition: usize,
        ctx: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let stream = self.partitions[partition].execute(ctx);
        let projected_stream = match self.projection.clone() {
            Some(projection) => Box::pin(RecordBatchStreamAdapter::new(
                Arc::clone(&self.projected_schema),
                stream.map(move |x| {
                    x.and_then(|b| b.project(projection.as_ref()).map_err(Into::into))
                }),
            )),
            None => stream,
        };
        Ok(match self.limit {
            None => projected_stream,
            Some(fetch) => {
                let baseline_metrics = BaselineMetrics::new(&self.metrics, partition);
                Box::pin(LimitStream::new(
                    projected_stream,
                    0,
                    Some(fetch),
                    baseline_metrics,
                ))
            }
        })
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn with_fetch(&self, limit: Option<usize>) -> Option<Arc<dyn ExecutionPlan>> {
        Some(Arc::new(DenormalizedStreamingTableExec {
            partitions: self.partitions.clone(),
            projection: self.projection.clone(),
            projected_schema: Arc::clone(&self.projected_schema),
            projected_output_ordering: self.projected_output_ordering.clone(),
            infinite: self.infinite,
            limit,
            cache: self.cache.clone(),
            metrics: self.metrics.clone(),
        }))
    }

    fn with_node_id(self: Arc<Self>, _node_id: usize) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let new_partitions: Vec<Arc<dyn PartitionStreamExt>> = self
            .partitions
            .iter()
            .map(|partition| {
                if partition.requires_node_id() {
                    if let Some(kafka_stream) = partition.as_partition_with_node_id() {
                        let new_stream = kafka_stream.clone().with_node_id(Some(_node_id));
                        Arc::new(new_stream) as Arc<dyn PartitionStreamExt>
                    } else {
                        Arc::clone(partition)
                    }
                } else {
                    Arc::clone(partition)
                }
            })
            .collect();
        let mut new_plan = DenormalizedStreamingTableExec {
            partitions: new_partitions,
            projection: self.projection.clone(),
            projected_schema: Arc::clone(&self.projected_schema),
            projected_output_ordering: self.projected_output_ordering.clone(),
            infinite: self.infinite,
            limit: self.limit,
            cache: self.cache.clone(),
            metrics: self.metrics.clone(),
        };
        let new_props = new_plan.cache.clone().with_node_id(_node_id);
        new_plan.cache = new_props;
        Ok(Some(Arc::new(new_plan)))
    }
}
