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

//! Wal replayer

use std::{
    collections::{HashMap, VecDeque},
    fmt::Display,
    ops::Range,
    sync::Arc,
    time::Instant,
};

use async_trait::async_trait;
use common_types::{
    schema::{IndexInWriterSchema, Schema},
    table::ShardId,
};
use futures::StreamExt;
use generic_error::BoxError;
use lazy_static::lazy_static;
use logger::{debug, error, info, trace, warn};
use prometheus::{exponential_buckets, register_histogram, Histogram};
use snafu::ResultExt;
use table_engine::table::TableId;
use tokio::sync::{Mutex, MutexGuard};
use wal::{
    log_batch::LogEntry,
    manager::{
        ReadBoundary, ReadContext, ReadRequest, RegionId, ScanContext, ScanRequest, WalManagerRef,
    },
};

use crate::{
    instance::{
        self,
        engine::{Error, ReplayWalWithCause, Result},
        flush_compaction::{Flusher, TableFlushOptions},
        serial_executor::TableOpSerialExecutor,
        write::{Error as WriteError, MemTableWriter},
    },
    payload::{ReadPayload, SingleSchemaProviderAdapter, TableSchemaProvider, WalDecoder},
    table::data::TableDataRef,
    ErrorKind,
};

// Metrics of wal replayer
lazy_static! {
    static ref PULL_LOGS_DURATION_HISTOGRAM: Histogram = register_histogram!(
        "wal_replay_pull_logs_duration",
        "Histogram for pull logs duration in wal replay in seconds",
        exponential_buckets(0.01, 2.0, 13).unwrap()
    )
    .unwrap();
    static ref APPLY_LOGS_DURATION_HISTOGRAM: Histogram = register_histogram!(
        "wal_replay_apply_logs_duration",
        "Histogram for apply logs duration in wal replay in seconds",
        exponential_buckets(0.01, 2.0, 13).unwrap()
    )
    .unwrap();
}

/// Wal replayer supporting both table based and region based
// TODO: limit the memory usage in `RegionBased` mode.
pub struct WalReplayer<'a> {
    context: ReplayContext,
    replay: Box<dyn Replay>,
    table_datas: &'a [TableDataRef],
}

impl<'a> WalReplayer<'a> {
    pub fn new(
        table_datas: &'a [TableDataRef],
        shard_id: ShardId,
        wal_manager: WalManagerRef,
        wal_replay_batch_size: usize,
        flusher: Flusher,
        max_retry_flush_limit: usize,
        replay_mode: ReplayMode,
    ) -> Self {
        let context = ReplayContext {
            shard_id,
            wal_manager,
            wal_replay_batch_size,
            flusher,
            max_retry_flush_limit,
        };

        let replay = Self::build_replay(replay_mode);

        Self {
            replay,
            context,
            table_datas,
        }
    }

    fn build_replay(mode: ReplayMode) -> Box<dyn Replay> {
        info!("Replay wal in mode:{mode:?}");

        match mode {
            ReplayMode::RegionBased => Box::new(RegionBasedReplay),
            ReplayMode::TableBased => Box::new(TableBasedReplay),
        }
    }

    /// Replay tables and return the failed tables and the causes.
    pub async fn replay(&mut self) -> Result<FailedTables> {
        // Build replay action according to mode.
        let table_num = self.table_datas.len();
        info!(
            "Replay wal logs begin, context:{}, table_num:{table_num}, tables:{:?}",
            self.context, self.table_datas
        );
        let begin = Instant::now();
        let result = self.replay.run(&self.context, self.table_datas).await;
        let cost = Instant::now().duration_since(begin);
        info!("Replay wal logs finish, table_num:{table_num}, cost:{cost:?}");

        result
    }
}

pub struct ReplayContext {
    pub shard_id: ShardId,
    pub wal_manager: WalManagerRef,
    pub wal_replay_batch_size: usize,
    pub flusher: Flusher,
    pub max_retry_flush_limit: usize,
}

impl Display for ReplayContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReplayContext")
            .field("shard_id", &self.shard_id)
            .field("replay_batch_size", &self.wal_replay_batch_size)
            .field("max_retry_flush_limit", &self.max_retry_flush_limit)
            .finish()
    }
}

#[derive(Debug, Clone, Copy)]
pub enum ReplayMode {
    RegionBased,
    TableBased,
}

pub type FailedTables = HashMap<TableId, Error>;

/// Replay action, the abstract of different replay strategies
#[async_trait]
trait Replay: Send + Sync + 'static {
    async fn run(
        &self,
        context: &ReplayContext,
        table_datas: &[TableDataRef],
    ) -> Result<FailedTables>;
}

/// Table based wal replay
struct TableBasedReplay;

#[async_trait]
impl Replay for TableBasedReplay {
    async fn run(
        &self,
        context: &ReplayContext,
        table_datas: &[TableDataRef],
    ) -> Result<FailedTables> {
        debug!("Replay wal logs on table mode, context:{context}, tables:{table_datas:?}",);

        let mut failed_tables = HashMap::new();
        let read_ctx = ReadContext {
            batch_size: context.wal_replay_batch_size,
            ..Default::default()
        };

        let mut tasks = futures::stream::iter(
            table_datas
                .iter()
                .map(|table_data| {
                    let table_id = table_data.id;
                    let read_ctx = &read_ctx;
                    async move {
                        let ret = Self::recover_table_logs(context, table_data, read_ctx).await;
                        (table_id, ret)
                    }
                })
                .collect::<Vec<_>>(),
        )
        .buffer_unordered(20);
        while let Some((table_id, ret)) = tasks.next().await {
            if let Err(e) = ret {
                // If occur error, mark this table as failed and store the cause.
                failed_tables.insert(table_id, e);
            }
        }

        Ok(failed_tables)
    }
}

impl TableBasedReplay {
    async fn recover_table_logs(
        context: &ReplayContext,
        table_data: &TableDataRef,
        read_ctx: &ReadContext,
    ) -> Result<()> {
        let table_location = table_data.table_location();
        let wal_location =
            instance::create_wal_location(table_location.id, table_location.shard_info);
        let read_req = ReadRequest {
            location: wal_location,
            start: ReadBoundary::Excluded(table_data.current_version().flushed_sequence()),
            end: ReadBoundary::Max,
        };

        // Read all wal of current table.
        let mut log_iter = context
            .wal_manager
            .read_batch(read_ctx, &read_req)
            .await
            .box_err()
            .context(ReplayWalWithCause { msg: None })?;

        let mut serial_exec = table_data.serial_exec.lock().await;
        let mut log_entry_buf = VecDeque::with_capacity(context.wal_replay_batch_size);
        loop {
            // fetch entries to log_entry_buf
            let _timer = PULL_LOGS_DURATION_HISTOGRAM.start_timer();
            let adapter = SingleSchemaProviderAdapter {
                schema: table_data.schema(),
            };
            let decoder = WalDecoder::new(adapter);
            // All the logs should belong the table, so no need to check again.
            let filter = |_| true;
            log_entry_buf = log_iter
                .next_log_entries(decoder, filter, log_entry_buf)
                .await
                .box_err()
                .context(ReplayWalWithCause { msg: None })?;

            if log_entry_buf.is_empty() {
                break;
            }

            // Replay all log entries of current table
            let _timer = APPLY_LOGS_DURATION_HISTOGRAM.start_timer();
            replay_table_log_entries(
                &context.flusher,
                context.max_retry_flush_limit,
                &mut serial_exec,
                table_data,
                log_entry_buf.iter(),
            )
            .await?;
        }

        Ok(())
    }
}

/// Region based wal replay
struct RegionBasedReplay;

#[async_trait]
impl Replay for RegionBasedReplay {
    async fn run(
        &self,
        context: &ReplayContext,
        table_datas: &[TableDataRef],
    ) -> Result<FailedTables> {
        debug!("Replay wal logs on region mode, context:{context}, tables:{table_datas:?}",);

        // Init all table results to be oks, and modify to errs when failed to replay.
        let mut failed_tables = FailedTables::new();
        let scan_ctx = ScanContext {
            batch_size: context.wal_replay_batch_size,
            ..Default::default()
        };

        Self::replay_region_logs(context, table_datas, &scan_ctx, &mut failed_tables).await?;

        Ok(failed_tables)
    }
}

#[derive(Clone)]
struct TableSchemaProviderAdapter {
    table_datas: Arc<HashMap<common_types::table::TableId, TableDataRef>>,
}

impl TableSchemaProvider for TableSchemaProviderAdapter {
    fn table_schema(&self, table_id: common_types::table::TableId) -> Option<Schema> {
        self.table_datas.get(&table_id).map(|v| v.schema())
    }
}

impl RegionBasedReplay {
    /// Replay logs in same region.
    ///
    /// Steps:
    /// + Scan all logs of region.
    /// + Split logs according to table ids.
    /// + Replay logs to recover data of tables.
    async fn replay_region_logs(
        context: &ReplayContext,
        table_datas: &[TableDataRef],
        scan_ctx: &ScanContext,
        failed_tables: &mut FailedTables,
    ) -> Result<()> {
        // Scan all wal logs of current shard.
        let scan_req = ScanRequest {
            region_id: context.shard_id as RegionId,
        };

        let mut log_iter = context
            .wal_manager
            .scan(scan_ctx, &scan_req)
            .await
            .box_err()
            .context(ReplayWalWithCause { msg: None })?;
        let mut log_entry_buf = VecDeque::with_capacity(context.wal_replay_batch_size);

        // Lock all related tables.
        let mut serial_exec_ctxs = HashMap::with_capacity(table_datas.len());
        let mut table_datas_by_id = HashMap::with_capacity(table_datas.len());
        for table_data in table_datas {
            let serial_exec = table_data.serial_exec.lock().await;
            let serial_exec_ctx = SerialExecContext {
                table_data: table_data.clone(),
                serial_exec,
            };
            serial_exec_ctxs.insert(table_data.id, serial_exec_ctx);
            table_datas_by_id.insert(table_data.id.as_u64(), table_data.clone());
        }

        let table_datas_by_id = Arc::new(table_datas_by_id);
        let schema_provider = TableSchemaProviderAdapter {
            table_datas: table_datas_by_id.clone(),
        };
        let serial_exec_ctxs = Arc::new(Mutex::new(serial_exec_ctxs));
        // Split and replay logs.
        loop {
            let _timer = PULL_LOGS_DURATION_HISTOGRAM.start_timer();
            let decoder = WalDecoder::new(schema_provider.clone());
            let table_datas_for_filter = table_datas_by_id.clone();
            let log_filter = move |log_table_id| table_datas_for_filter.contains_key(&log_table_id);
            log_entry_buf = log_iter
                .next_log_entries(decoder, log_filter, log_entry_buf)
                .await
                .box_err()
                .context(ReplayWalWithCause { msg: None })?;

            if log_entry_buf.is_empty() {
                break;
            }

            let _timer = APPLY_LOGS_DURATION_HISTOGRAM.start_timer();
            Self::replay_single_batch(context, &log_entry_buf, &serial_exec_ctxs, failed_tables)
                .await?;
        }

        Ok(())
    }

    async fn replay_single_batch(
        context: &ReplayContext,
        log_batch: &VecDeque<LogEntry<ReadPayload>>,
        serial_exec_ctxs: &Arc<Mutex<HashMap<TableId, SerialExecContext<'_>>>>,
        failed_tables: &mut FailedTables,
    ) -> Result<()> {
        let mut table_batches = Vec::new();
        // TODO: No `group_by` method in `VecDeque`, so implement it manually here...
        Self::split_log_batch_by_table(log_batch, &mut table_batches);

        // TODO: Replay logs of different tables in parallel.
        let mut replay_tasks = Vec::with_capacity(table_batches.len());
        for table_batch in table_batches {
            // Some tables may have failed in previous replay, ignore them.
            if failed_tables.contains_key(&table_batch.table_id) {
                continue;
            }
            let log_entries: Vec<_> = table_batch
                .ranges
                .iter()
                .flat_map(|range| log_batch.range(range.clone()))
                .collect();

            let serial_exec_ctxs = serial_exec_ctxs.clone();
            replay_tasks.push(async move {
                // Some tables may have been moved to other shards or dropped, ignore such logs.
                if let Some(ctx) = serial_exec_ctxs.lock().await.get_mut(&table_batch.table_id) {
                    let result = replay_table_log_entries(
                        &context.flusher,
                        context.max_retry_flush_limit,
                        &mut ctx.serial_exec,
                        &ctx.table_data,
                        log_entries.into_iter(),
                    )
                    .await;
                    (table_batch.table_id, Some(result))
                } else {
                    (table_batch.table_id, None)
                }
            });
        }

        // Run at most 20 tasks in parallel
        let mut replay_tasks = futures::stream::iter(replay_tasks).buffer_unordered(20);
        while let Some((table_id, ret)) = replay_tasks.next().await {
            if let Some(Err(e)) = ret {
                // If occur error, mark this table as failed and store the cause.
                failed_tables.insert(table_id, e);
            }
        }

        Ok(())
    }

    fn split_log_batch_by_table<P>(
        log_batch: &VecDeque<LogEntry<P>>,
        table_batches: &mut Vec<TableBatch>,
    ) {
        table_batches.clear();

        if log_batch.is_empty() {
            return;
        }

        // Split log batch by table id, for example:
        // input batch:
        //  |1|1|2|2|2|3|3|3|3|1|1|
        //
        // output batches:
        //  |1|1|1|1|, |2|2|2|, |3|3|3|3|
        let mut start_log_idx = 0usize;
        let mut curr_log_idx = 0usize;
        let mut start_table_id = log_batch.get(start_log_idx).unwrap().table_id;
        let mut table_ranges = HashMap::new();
        loop {
            let time_to_break = curr_log_idx == log_batch.len();
            let found_end_idx = if time_to_break {
                true
            } else {
                let current_table_id = log_batch.get(curr_log_idx).unwrap().table_id;
                current_table_id != start_table_id
            };

            if found_end_idx {
                table_ranges
                    .entry(TableId::new(start_table_id))
                    .or_insert(Vec::new())
                    .push(start_log_idx..curr_log_idx);

                // Step to next start idx.
                start_log_idx = curr_log_idx;
                start_table_id = if time_to_break {
                    // The final round, just set it to max as an invalid flag.
                    u64::MAX
                } else {
                    log_batch.get(start_log_idx).unwrap().table_id
                };
            }

            if time_to_break {
                break;
            }
            curr_log_idx += 1;
        }
        for (table_id, ranges) in table_ranges {
            table_batches.push(TableBatch { table_id, ranges });
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
struct TableBatch {
    table_id: TableId,
    ranges: Vec<Range<usize>>,
}

struct SerialExecContext<'a> {
    table_data: TableDataRef,
    serial_exec: MutexGuard<'a, TableOpSerialExecutor>,
}

/// Replay all log entries into memtable and flush if necessary
async fn replay_table_log_entries(
    flusher: &Flusher,
    max_retry_flush_limit: usize,
    serial_exec: &mut TableOpSerialExecutor,
    table_data: &TableDataRef,
    log_entries: impl Iterator<Item = &LogEntry<ReadPayload>>,
) -> Result<()> {
    let flushed_sequence = table_data.current_version().flushed_sequence();
    debug!(
        "Replay table log entries begin, table:{}, table_id:{:?}, last_sequence:{}, flushed_sequence:{flushed_sequence}",
        table_data.name, table_data.id, table_data.last_sequence(),
    );

    for log_entry in log_entries {
        let (sequence, payload) = (log_entry.sequence, &log_entry.payload);

        // Ignore too old logs(sequence <= `flushed_sequence`).
        if sequence <= flushed_sequence {
            continue;
        }

        // Apply logs to memtable.
        match payload {
            ReadPayload::Write { row_group } => {
                trace!(
                    "Instance replay row_group, table:{}, row_group:{:?}",
                    table_data.name,
                    row_group
                );

                // TODO: too strict check here, should be modified to like what in
                // `ColumnSchema::compatible_for_write`.`
                let table_schema_version = table_data.schema_version();
                if table_schema_version != row_group.schema().version() {
                    // Data with old schema should already been flushed, but we avoid panic
                    // here.
                    error!(
                        "Ignore data with mismatch schema version during replaying, \
                        table:{}, \
                        table_id:{:?}, \
                        expect:{}, \
                        actual:{}, \
                        last_sequence:{}, \
                        sequence:{}",
                        table_data.name,
                        table_data.id,
                        table_schema_version,
                        row_group.schema().version(),
                        table_data.last_sequence(),
                        sequence,
                    );

                    continue;
                }

                let index_in_writer =
                    IndexInWriterSchema::for_same_schema(row_group.schema().num_columns());
                let memtable_writer = MemTableWriter::new(table_data.clone(), serial_exec);
                let write_res = memtable_writer.write(sequence, row_group, index_in_writer);
                if let Err(e) = write_res {
                    if matches!(e, WriteError::UpdateMemTableSequence { ref source } if source.kind() == ErrorKind::KeyTooLarge )
                    {
                        // ignore this error
                        warn!("Unable to insert memtable, err:{e}");
                    } else {
                        return Err(Error::ReplayWalWithCause {
                            msg: Some(format!(
                                "table_id:{}, table_name:{}, space_id:{}",
                                table_data.space_id, table_data.name, table_data.id
                            )),
                            source: Box::new(e),
                        });
                    }
                }

                // Flush the table if necessary.
                let in_flush = serial_exec.flush_scheduler().is_in_flush();
                if table_data.should_flush_table(in_flush) {
                    let opts = TableFlushOptions {
                        res_sender: None,
                        max_retry_flush_limit,
                    };
                    let flush_scheduler = serial_exec.flush_scheduler();
                    flusher
                        .schedule_flush(flush_scheduler, table_data, opts)
                        .await
                        .box_err()
                        .context(ReplayWalWithCause {
                            msg: Some(format!(
                                "table_id:{}, table_name:{}, space_id:{}",
                                table_data.space_id, table_data.name, table_data.id
                            )),
                        })?;
                }
            }
            ReadPayload::AlterSchema { .. } | ReadPayload::AlterOptions { .. } => {
                // Ignore records except Data.
                //
                // - DDL (AlterSchema and AlterOptions) should be recovered from
                //   Manifest on start.
            }
        }

        table_data.set_last_sequence(sequence);
    }

    debug!(
        "Replay table log entries finish, table:{}, table_id:{:?}, last_sequence:{}, flushed_sequence:{}",
        table_data.name, table_data.id, table_data.last_sequence(), table_data.current_version().flushed_sequence()
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use table_engine::table::TableId;
    use wal::log_batch::LogEntry;

    use crate::instance::wal_replayer::{RegionBasedReplay, TableBatch};

    #[test]
    fn test_split_log_batch_by_table() {
        let test_set = test_set();
        for (test_batch, expected) in test_set {
            check_split_result(&test_batch, &expected);
        }
    }

    #[allow(clippy::single_range_in_vec_init)]
    fn test_set() -> Vec<(VecDeque<LogEntry<u32>>, Vec<TableBatch>)> {
        let test_log_batch1: VecDeque<LogEntry<u32>> = VecDeque::from([
            LogEntry {
                table_id: 0,
                sequence: 1,
                payload: 0,
            },
            LogEntry {
                table_id: 0,
                sequence: 2,
                payload: 0,
            },
            LogEntry {
                table_id: 0,
                sequence: 3,
                payload: 0,
            },
            LogEntry {
                table_id: 1,
                sequence: 1,
                payload: 0,
            },
            LogEntry {
                table_id: 1,
                sequence: 2,
                payload: 0,
            },
            LogEntry {
                table_id: 2,
                sequence: 1,
                payload: 0,
            },
        ]);
        let expected1 = vec![
            TableBatch {
                table_id: TableId::new(0),
                ranges: vec![0..3],
            },
            TableBatch {
                table_id: TableId::new(1),
                ranges: vec![3..5],
            },
            TableBatch {
                table_id: TableId::new(2),
                ranges: vec![5..6],
            },
        ];

        let test_log_batch2: VecDeque<LogEntry<u32>> = VecDeque::from([LogEntry {
            table_id: 0,
            sequence: 1,
            payload: 0,
        }]);
        let expected2 = vec![TableBatch {
            table_id: TableId::new(0),
            ranges: vec![0..1],
        }];

        let test_log_batch3: VecDeque<LogEntry<u32>> = VecDeque::default();
        let expected3 = vec![];

        vec![
            (test_log_batch1, expected1),
            (test_log_batch2, expected2),
            (test_log_batch3, expected3),
        ]
    }

    fn check_split_result(batch: &VecDeque<LogEntry<u32>>, expected: &[TableBatch]) {
        let mut table_batches = Vec::new();
        RegionBasedReplay::split_log_batch_by_table(batch, &mut table_batches);
        // split_log_batch_by_table returns unordered results, so sort it here.
        table_batches.sort_by_key(|tb| tb.table_id);
        assert_eq!(&table_batches, expected);
    }
}
