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
use std::{collections::HashMap, sync::Arc};

use datafusion::{error::DataFusionError, physical_plan::ExecutionPlan};

// Util for traversing ExecutionPlan tree and annotating node_id
pub struct NodeIdAnnotator {
    next_id: usize,
}

impl NodeIdAnnotator {
    pub fn new() -> Self {
        NodeIdAnnotator { next_id: 0 }
    }

    pub fn next_node_id(&mut self) -> usize {
        let node_id = self.next_id;
        self.next_id += 1;
        node_id
    }
}

pub fn annotate_node_id_for_execution_plan(
    plan: &Arc<dyn ExecutionPlan>,
    annotator: &mut NodeIdAnnotator,
    plan_map: &mut HashMap<usize, usize>,
) {
    for child in plan.children() {
        annotate_node_id_for_execution_plan(child, annotator, plan_map);
    }
    let node_id = annotator.next_node_id();
    let addr = Arc::as_ptr(plan) as *const () as usize;
    plan_map.insert(addr, node_id);
}
