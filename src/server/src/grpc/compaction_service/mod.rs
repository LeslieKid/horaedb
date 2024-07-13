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

// Compaction rpc service implementation.

use std::sync::Arc;

use analytic_engine::compaction::runner::{CompactionRunner, CompactionRunnerResult, CompactionRunnerTask};
use analytic_engine::instance::flush_compaction::Result;
use analytic_engine::memtable::factory::FactoryRef;
use analytic_engine::sst::factory::{ObjectStorePickerRef, ScanOptions};
use runtime::Runtime;

mod error;

/// Executor carrying for actual compaction work
pub struct RemoteCompactionRunner {
    runtime: Arc<Runtime>,
    scan_options: ScanOptions,
    /// Sst factory
    sst_factory: FactoryRef,
    /// Store picker for persisting sst
    store_picker: ObjectStorePickerRef,
    // TODO
}

impl RemoteCompactionRunner {
    pub fn new() -> Self {
        unimplemented!()
    }
}

impl CompactionRunner for RemoteCompactionRunner {
    async fn run(&self,task: CompactionRunnerTask) -> Result<CompactionRunnerResult> {
        unimplemented!() 
    }
}

#[derive(Clone)]
pub struct CompactionServiceImpl {
    runtime: Arc<Runtime>,
    // TODO
}

#[async_trait]
impl CompactionService for CompactionServiceImpl {
    async fn execute_compaction_task(
        &self, 
        request: tonic::Request<ExecuteCompactionTaskRequest>,
    ) -> Result<
        tonic::Response<ExecuteCompactionTaskResponse>,
        tonic::Status,
    > {
        // request --> CompactionRunnerTask --> RemoteCompactionRunner.run()
        // --> CompactionRunnerResult --> ExecuteCompactionTaskResponse
        unimplemented!()
    }
}
