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

//! CompactionCluster sub-crate includes several functionalities for supporting Compaction
//! server to running in the distribute mode. Including:
//! - (todo) Request HoraeMeta for reading topology or configuration.
//! - Accept HoraeDB's commands like compact.
//!
//! The core types are [CompactionCluster] trait and its implementation 
//! [CompactionClusterImpl].

#![feature(trait_alias)]

use std::sync::Arc;

use analytic_engine::{
    instance::flush_compaction, 
    compaction::runner::{CompactionRunnerResult, CompactionRunnerTask}
};
use async_trait::async_trait;
use snafu::Snafu;
use macros::define_result;

pub mod cluster_impl;

#[derive(Debug, Snafu)]
#[snafu(visibility = "pub")]
pub enum Error {}

define_result!(Error);

pub type CompactionClusterRef = Arc<dyn CompactionCluster + Send + Sync>;

#[async_trait]
pub trait CompactionCluster {
    async fn start(&self) -> Result<()>;
    async fn stop(&self) -> Result<()>;

    async fn compact(&self, task: CompactionRunnerTask) -> flush_compaction::Result<CompactionRunnerResult>;
}