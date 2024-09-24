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

//! Setup server

use std::sync::Arc;

use analytic_engine::{
    self,
    setup::{EngineBuilder, TableEngineContext},
};
use catalog::{manager::ManagerRef, schema::OpenOptions, table_operator::TableOperator};
use catalog_impls::{table_based::TableBasedManager, volatile, CatalogManagerImpl};
use cluster::{cluster_impl::ClusterImpl, config::ClusterConfig, shard_set::ShardSet};
use common_types::cluster::NodeType;
use datafusion::execution::runtime_env::RuntimeConfig as DfRuntimeConfig;
use df_operator::registry::{FunctionRegistry, FunctionRegistryImpl};
use interpreters::table_manipulator::{catalog_based, meta_based};
use logger::{info, warn, RuntimeLevel};
use meta_client::{meta_impl, types::NodeMetaInfo};
use proxy::{
    limiter::Limiter,
    schema_config_provider::{
        cluster_based::ClusterBasedProvider, config_based::ConfigBasedProvider,
    },
};
use router::{rule_based::ClusterView, ClusterBasedRouter, RuleBasedRouter};
use runtime::PriorityRuntime;
use server::{
    config::{StaticRouteConfig, StaticTopologyConfig},
    local_tables::LocalTablesRecoverer,
    server::{Builder, DatafusionContext},
};
use table_engine::{
    engine::{EngineRuntimes, TableEngineRef},
    memory::MemoryTableEngine,
    proxy::TableEngineProxy,
};
use tracing_util::{
    self,
    tracing_appender::{non_blocking::WorkerGuard, rolling::Rotation},
};
use wal::{
    config::StorageConfig,
    manager::{WalRuntimes, WalsOpener},
};

use crate::{
    config::{ClusterDeployment, Config, RuntimeConfig},
    signal_handler,
};

/// Setup log with given `config`, returns the runtime log level switch.
pub fn setup_logger(config: &Config) -> RuntimeLevel {
    logger::init_log(&config.logger).expect("Failed to init log.")
}

/// Setup tracing with given `config`, returns the writer guard.
pub fn setup_tracing(config: &Config) -> WorkerGuard {
    tracing_util::init_tracing_with_file(&config.tracing, &config.node.addr, Rotation::NEVER)
}

fn build_runtime_with_stack_size(
    name: &str,
    threads_num: usize,
    stack_size: Option<usize>,
) -> runtime::Runtime {
    let mut builder = runtime::Builder::default();

    if let Some(stack_size) = stack_size {
        builder.stack_size(stack_size);
    }

    builder
        .worker_threads(threads_num)
        .thread_name(name)
        .enable_all()
        .build()
        .expect("Failed to create runtime")
}

fn build_runtime(name: &str, threads_num: usize) -> runtime::Runtime {
    build_runtime_with_stack_size(name, threads_num, None)
}

fn build_engine_runtimes(config: &RuntimeConfig) -> EngineRuntimes {
    let read_stack_size = config.read_thread_stack_size.as_byte() as usize;
    EngineRuntimes {
        read_runtime: PriorityRuntime::new(
            Arc::new(build_runtime_with_stack_size(
                "read-low",
                config.low_read_thread_num,
                Some(read_stack_size),
            )),
            Arc::new(build_runtime_with_stack_size(
                "read-high",
                config.read_thread_num,
                Some(read_stack_size),
            )),
        ),
        write_runtime: Arc::new(build_runtime("horaedb-write", config.write_thread_num)),
        compact_runtime: Arc::new(build_runtime("horaedb-compact", config.compact_thread_num)),
        meta_runtime: Arc::new(build_runtime("horaedb-meta", config.meta_thread_num)),
        default_runtime: Arc::new(build_runtime("horaedb-default", config.default_thread_num)),
        io_runtime: Arc::new(build_runtime("horaedb-io", config.io_thread_num)),
    }
}

fn validate_config(config: &Config) {
    let is_data_wal_disabled = config.analytic.wal.disable_data;
    if is_data_wal_disabled {
        warn!("disable data wal may cause data loss, please check whether this configuration is correct")
    }
}

/// Run a server, returns when the server is shutdown by user
pub fn run_server(config: Config, log_runtime: RuntimeLevel) {
    let runtimes = Arc::new(build_engine_runtimes(&config.runtime));
    let engine_runtimes = runtimes.clone();
    let log_runtime = Arc::new(log_runtime);

    info!("Server starts up, config:{:#?}", config);

    validate_config(&config);

    runtimes.default_runtime.block_on(async {
        match config.analytic.wal.storage {
            StorageConfig::RocksDB(_) => {
                #[cfg(feature = "wal-rocksdb")]
                {
                    use wal::rocksdb_impl::manager::RocksDBWalsOpener;
                    run_server_with_runtimes::<RocksDBWalsOpener>(
                        config,
                        engine_runtimes,
                        log_runtime,
                    )
                    .await
                }
                #[cfg(not(feature = "wal-rocksdb"))]
                {
                    panic!("RocksDB WAL not bundled!");
                }
            }

            StorageConfig::Obkv(_) => {
                #[cfg(feature = "wal-table-kv")]
                {
                    use wal::table_kv_impl::wal::ObkvWalsOpener;
                    run_server_with_runtimes::<ObkvWalsOpener>(
                        config,
                        engine_runtimes,
                        log_runtime,
                    )
                    .await;
                }
                #[cfg(not(feature = "wal-table-kv"))]
                {
                    panic!("Table KV WAL not bundled!");
                }
            }

            StorageConfig::Kafka(_) => {
                #[cfg(feature = "wal-message-queue")]
                {
                    use wal::message_queue_impl::wal::KafkaWalsOpener;
                    run_server_with_runtimes::<KafkaWalsOpener>(
                        config,
                        engine_runtimes,
                        log_runtime,
                    )
                    .await;
                }
                #[cfg(not(feature = "wal-message-queue"))]
                {
                    panic!("Message Queue WAL not bundled!");
                }
            }
            StorageConfig::Local(_) => {
                #[cfg(feature = "wal-local-storage")]
                {
                    use wal::local_storage_impl::wal_manager::LocalStorageWalsOpener;
                    run_server_with_runtimes::<LocalStorageWalsOpener>(
                        config,
                        engine_runtimes,
                        log_runtime,
                    )
                    .await;
                }
                #[cfg(not(feature = "wal-local-storage"))]
                {
                    panic!("Local Storage WAL not bundled!");
                }
            }
        }
    });
}

async fn run_server_with_runtimes<T>(
    config: Config,
    engine_runtimes: Arc<EngineRuntimes>,
    log_runtime: Arc<RuntimeLevel>,
) where
    T: WalsOpener,
{
    // Init function registry.
    let mut function_registry = FunctionRegistryImpl::new();
    function_registry
        .load_functions()
        .expect("Failed to create function registry");
    let function_registry = Arc::new(function_registry);
    let datafusion_context = DatafusionContext {
        function_registry: function_registry.clone().to_df_function_registry(),
        runtime_config: DfRuntimeConfig::default(),
    };

    // Config limiter
    let limiter = Limiter::new(config.limiter.clone());
    let config_content = toml::to_string(&config).expect("Fail to serialize config");

    let builder = Builder::new(config.server.clone())
        .node_addr(config.node.addr.clone())
        .config_content(config_content)
        .engine_runtimes(engine_runtimes.clone())
        .log_runtime(log_runtime.clone())
        .function_registry(function_registry)
        .limiter(limiter)
        .datafusion_context(datafusion_context)
        .query_engine_config(config.query_engine.clone());

    let wal_builder = T::default();
    let builder = match &config.cluster_deployment {
        None => {
            build_without_meta(
                &config,
                &StaticRouteConfig::default(),
                builder,
                engine_runtimes.clone(),
                wal_builder,
            )
            .await
        }
        Some(ClusterDeployment::NoMeta(v)) => {
            build_without_meta(&config, v, builder, engine_runtimes.clone(), wal_builder).await
        }
        Some(ClusterDeployment::WithMeta(cluster_config)) => {
            build_with_meta(
                &config,
                cluster_config,
                builder,
                engine_runtimes.clone(),
                wal_builder,
            )
            .await
        }
    };

    // Build and start server
    let mut server = builder.build().expect("Failed to create server");
    server.start().await.expect("Failed to start server");

    // Wait for signal
    signal_handler::wait_for_signal();

    // Stop server
    server.stop().await;
}

// Build proxy for all table engines.
async fn build_table_engine_proxy(analytic: TableEngineRef) -> Arc<TableEngineProxy> {
    // Create memory engine
    let memory = MemoryTableEngine;

    // Create table engine proxy
    Arc::new(TableEngineProxy {
        memory,
        analytic: analytic.clone(),
    })
}

fn make_wal_runtime(runtimes: Arc<EngineRuntimes>) -> WalRuntimes {
    WalRuntimes {
        write_runtime: runtimes.write_runtime.clone(),
        // TODO: remove read_runtime from WalRuntimes
        read_runtime: runtimes.read_runtime.high().clone(),
        default_runtime: runtimes.default_runtime.clone(),
    }
}

async fn build_with_meta<T: WalsOpener>(
    config: &Config,
    cluster_config: &ClusterConfig,
    builder: Builder,
    runtimes: Arc<EngineRuntimes>,
    wal_opener: T,
) -> Builder {
    // Build meta related modules.
    let node_meta_info = NodeMetaInfo {
        addr: config.node.addr.clone(),
        port: config.server.grpc_port,
        zone: config.node.zone.clone(),
        idc: config.node.idc.clone(),
        binary_version: config.node.binary_version.clone(),
        node_type: cluster_config.node_type.clone(),
    };

    info!("Build horaedb with node meta info:{node_meta_info:?}");

    let endpoint = node_meta_info.endpoint();
    let meta_client =
        meta_impl::build_meta_client(cluster_config.meta_client.clone(), node_meta_info)
            .await
            .expect("fail to build meta client");

    let shard_set = ShardSet::default();
    let cluster = {
        let cluster_impl = ClusterImpl::try_new(
            endpoint,
            shard_set.clone(),
            meta_client.clone(),
            cluster_config.clone(),
            runtimes.meta_runtime.clone(),
        )
        .await
        .unwrap();
        Arc::new(cluster_impl)
    };
    let router = Arc::new(ClusterBasedRouter::new(
        cluster.clone(),
        config.server.route_cache.clone(),
    ));

    let opened_wals = wal_opener
        .open_wals(&config.analytic.wal, make_wal_runtime(runtimes.clone()))
        .await
        .expect("Failed to setup analytic engine");
    let engine_builder = EngineBuilder {
        config: &config.analytic,
        engine_runtimes: runtimes.clone(),
        opened_wals: opened_wals.clone(),
    };
    let TableEngineContext {
        table_engine,
        local_compaction_runner,
    } = engine_builder
        .build()
        .await
        .expect("Failed to setup analytic engine");
    let engine_proxy = build_table_engine_proxy(table_engine).await;

    let meta_based_manager_ref = Arc::new(volatile::ManagerImpl::new(
        shard_set,
        meta_client.clone(),
        cluster.clone(),
    ));

    // Build catalog manager.
    let catalog_manager = Arc::new(CatalogManagerImpl::new(meta_based_manager_ref));

    let table_manipulator = Arc::new(meta_based::TableManipulatorImpl::new(meta_client));

    let schema_config_provider = Arc::new(ClusterBasedProvider::new(cluster.clone()));

    let mut builder = builder
        .table_engine(engine_proxy)
        .catalog_manager(catalog_manager)
        .table_manipulator(table_manipulator)
        .cluster(cluster)
        .opened_wals(opened_wals)
        .router(router)
        .schema_config_provider(schema_config_provider);
    if let NodeType::CompactionServer = cluster_config.node_type {
        builder =
            builder.compaction_runner(local_compaction_runner.expect("Empty compaction runner."));
    }
    builder
}

async fn build_without_meta<T: WalsOpener>(
    config: &Config,
    static_route_config: &StaticRouteConfig,
    builder: Builder,
    runtimes: Arc<EngineRuntimes>,
    wal_builder: T,
) -> Builder {
    let opened_wals = wal_builder
        .open_wals(&config.analytic.wal, make_wal_runtime(runtimes.clone()))
        .await
        .expect("Failed to setup analytic engine");

    let engine_builder = EngineBuilder {
        config: &config.analytic,
        engine_runtimes: runtimes.clone(),
        opened_wals: opened_wals.clone(),
    };
    let TableEngineContext { table_engine, .. } = engine_builder
        .build()
        .await
        .expect("Failed to setup analytic engine");
    let engine_proxy = build_table_engine_proxy(table_engine).await;

    // Create catalog manager, use analytic engine as backend.
    let analytic = engine_proxy.analytic.clone();
    let mut table_based_manager = TableBasedManager::new(analytic)
        .await
        .expect("Failed to create catalog manager");

    // Get collected table infos.
    let table_infos = table_based_manager
        .fetch_table_infos()
        .await
        .expect("Failed to fetch table infos for opening");

    let catalog_manager = Arc::new(CatalogManagerImpl::new(Arc::new(table_based_manager)));
    let table_operator = TableOperator::new(catalog_manager.clone());
    let table_manipulator = Arc::new(catalog_based::TableManipulatorImpl::new(
        table_operator.clone(),
    ));

    // Iterate the table infos to recover.
    let open_opts = OpenOptions {
        table_engine: engine_proxy.clone(),
    };

    // Create local tables recoverer.
    let local_tables_recoverer = LocalTablesRecoverer::new(table_infos, table_operator, open_opts);

    // Create schema in default catalog.
    create_static_topology_schema(
        catalog_manager.clone(),
        static_route_config.topology.clone(),
    )
    .await;

    // Build static router and schema config provider
    let cluster_view = ClusterView::from(&static_route_config.topology);
    let schema_configs = cluster_view.schema_configs.clone();
    let router = Arc::new(RuleBasedRouter::new(
        cluster_view,
        static_route_config.rules.clone(),
    ));
    let schema_config_provider = Arc::new(ConfigBasedProvider::new(
        schema_configs,
        config.server.default_schema_config.clone(),
    ));

    builder
        .table_engine(engine_proxy)
        .catalog_manager(catalog_manager)
        .table_manipulator(table_manipulator)
        .router(router)
        .opened_wals(opened_wals)
        .schema_config_provider(schema_config_provider)
        .local_tables_recoverer(local_tables_recoverer)
}

async fn create_static_topology_schema(
    catalog_mgr: ManagerRef,
    static_topology_config: StaticTopologyConfig,
) {
    let default_catalog = catalog_mgr
        .catalog_by_name(catalog_mgr.default_catalog_name())
        .expect("Fail to retrieve default catalog")
        .expect("Default catalog doesn't exist");
    for schema_shard_view in static_topology_config.schema_shards {
        default_catalog
            .create_schema(&schema_shard_view.schema)
            .await
            .unwrap_or_else(|_| panic!("Fail to create schema:{}", schema_shard_view.schema));
        info!(
            "Create static topology in default catalog:{}, schema:{}",
            catalog_mgr.default_catalog_name(),
            &schema_shard_view.schema
        );
    }
}
