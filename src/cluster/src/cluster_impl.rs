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

use std::{
    sync::{Arc, Mutex, RwLock},
    time::Duration,
};

use async_trait::async_trait;
use common_types::table::ShardId;
use compaction_client::{
    compaction_impl::{build_compaction_client, CompactionClientConfig},
    types::{ExecuteCompactionTaskRequest, ExecuteCompactionTaskResponse},
    CompactionClientRef,
};
use etcd_client::{Certificate, ConnectOptions, Identity, TlsOptions};
use generic_error::BoxError;
use logger::{error, info, warn};
use meta_client::{
    types::{
        GetNodesRequest, GetTablesOfShardsRequest, RouteTablesRequest, RouteTablesResponse,
        ShardInfo,
    },
    MetaClientRef,
};
use runtime::{JoinHandle, Runtime};
use snafu::{ensure, OptionExt, ResultExt};
use tokio::{
    fs, io,
    sync::mpsc::{self, Sender},
    time,
};

use crate::{
    config::{ClusterConfig, EtcdClientConfig},
    shard_lock_manager::{self, ShardLockManager, ShardLockManagerRef},
    shard_set::{Shard, ShardRef, ShardSet},
    topology::ClusterTopology,
    Cluster, ClusterNodesNotFound, ClusterNodesResp, ClusterType, CompactionClientFailure,
    CompactionOffloadNotAllowed, EtcdClientFailureWithCause, InitEtcdClientConfig,
    InvalidArguments, MetaClientFailure, OpenShard, OpenShardWithCause, Result, ShardNotFound,
    TableStatus,
};

/// ClusterImpl is an implementation of [`Cluster`] based [`MetaClient`].
///
/// Its functions are to:
///  - Handle the some action from the HoraeMeta;
///  - Handle the heartbeat between horaedb-server and HoraeMeta;
///  - Provide the cluster topology.
pub struct ClusterImpl {
    inner: Arc<Inner>,
    runtime: Arc<Runtime>,
    config: ClusterConfig,
    heartbeat_handle: Mutex<Option<JoinHandle<()>>>,
    stop_heartbeat_tx: Mutex<Option<Sender<()>>>,
    shard_lock_manager: ShardLockManagerRef,
}

impl ClusterImpl {
    pub async fn try_new(
        node_name: String,
        shard_set: ShardSet,
        meta_client: MetaClientRef,
        config: ClusterConfig,
        runtime: Arc<Runtime>,
    ) -> Result<Self> {
        if let Err(e) = config.etcd_client.validate() {
            return InvalidArguments { msg: e }.fail();
        }

        let connect_options = build_etcd_connect_options(&config.etcd_client)
            .await
            .context(InitEtcdClientConfig)?;
        let etcd_client =
            etcd_client::Client::connect(&config.etcd_client.server_addrs, Some(connect_options))
                .await
                .context(EtcdClientFailureWithCause {
                    msg: "failed to connect to etcd",
                })?;

        let shard_lock_key_prefix = Self::shard_lock_key_prefix(
            &config.etcd_client.root_path,
            &config.meta_client.cluster_name,
        )?;
        let shard_lock_mgr_config = shard_lock_manager::Config {
            node_name,
            lock_key_prefix: shard_lock_key_prefix,
            lock_lease_ttl_sec: config.etcd_client.shard_lock_lease_ttl_sec,
            lock_lease_check_interval: config.etcd_client.shard_lock_lease_check_interval.0,
            enable_fast_reacquire_lock: config.etcd_client.enable_shard_lock_fast_reacquire,
            rpc_timeout: config.etcd_client.rpc_timeout(),
            runtime: runtime.clone(),
        };
        let shard_lock_manager = ShardLockManager::new(shard_lock_mgr_config, etcd_client);

        let inner = Arc::new(Inner::new(shard_set, meta_client)?);
        Ok(Self {
            inner,
            runtime,
            config,
            heartbeat_handle: Mutex::new(None),
            stop_heartbeat_tx: Mutex::new(None),
            shard_lock_manager: Arc::new(shard_lock_manager),
        })
    }

    fn start_heartbeat_loop(&self) {
        let interval = self.heartbeat_interval();
        let error_wait_lease = self.error_wait_lease();
        let inner = self.inner.clone();
        let (tx, mut rx) = mpsc::channel(1);

        let handle = self.runtime.spawn(async move {
            loop {
                let shard_infos = inner
                    .shard_set
                    .all_shards()
                    .iter()
                    .map(|shard| shard.shard_info())
                    .collect();
                info!("Node heartbeat to meta, shard infos:{:?}", shard_infos);

                let resp = inner.meta_client.send_heartbeat(shard_infos).await;
                let wait = match resp {
                    Ok(()) => interval,
                    Err(e) => {
                        error!("Send heartbeat to meta failed, err:{}", e);
                        error_wait_lease
                    }
                };

                if time::timeout(wait, rx.recv()).await.is_ok() {
                    warn!("Receive exit command and exit heartbeat loop");
                    break;
                }
            }
        });

        *self.stop_heartbeat_tx.lock().unwrap() = Some(tx);
        *self.heartbeat_handle.lock().unwrap() = Some(handle);
    }

    // Register node every 2/3 lease
    fn heartbeat_interval(&self) -> Duration {
        Duration::from_millis(self.config.meta_client.lease.as_millis() * 2 / 3)
    }

    fn error_wait_lease(&self) -> Duration {
        self.config.meta_client.lease.0 / 2
    }

    fn shard_lock_key_prefix(root_path: &str, cluster_name: &str) -> Result<String> {
        ensure!(
            root_path.starts_with('/'),
            InvalidArguments {
                msg: "root_path is required to start with /",
            }
        );

        ensure!(
            !cluster_name.is_empty(),
            InvalidArguments {
                msg: "cluster_name is required non-empty",
            }
        );

        const SHARD_LOCK_KEY: &str = "shards";
        Ok(format!("{root_path}/{cluster_name}/{SHARD_LOCK_KEY}"))
    }
}

struct Inner {
    shard_set: ShardSet,
    meta_client: MetaClientRef,
    topology: RwLock<ClusterTopology>,
}

impl Inner {
    fn new(shard_set: ShardSet, meta_client: MetaClientRef) -> Result<Self> {
        Ok(Self {
            shard_set,
            meta_client,
            topology: Default::default(),
        })
    }

    async fn route_tables(&self, req: &RouteTablesRequest) -> Result<RouteTablesResponse> {
        // TODO: we should use self.topology to cache the route result to reduce the
        // pressure on the HoraeMeta.
        let route_resp = self
            .meta_client
            .route_tables(req.clone())
            .await
            .context(MetaClientFailure)?;

        Ok(route_resp)
    }

    async fn fetch_nodes(&self) -> Result<ClusterNodesResp> {
        {
            let topology = self.topology.read().unwrap();
            let cached_node_topology = topology.nodes();
            if let Some(cached_node_topology) = cached_node_topology {
                return Ok(ClusterNodesResp {
                    cluster_topology_version: cached_node_topology.version,
                    cluster_nodes: cached_node_topology.nodes,
                });
            }
        }

        let req = GetNodesRequest::default();
        let resp = self
            .meta_client
            .get_nodes(req)
            .await
            .context(MetaClientFailure)?;

        let version = resp.cluster_topology_version;
        let nodes = Arc::new(resp.node_shards);
        let updated = self
            .topology
            .write()
            .unwrap()
            .maybe_update_nodes(nodes.clone(), version);

        let resp = if updated {
            ClusterNodesResp {
                cluster_topology_version: version,
                cluster_nodes: nodes,
            }
        } else {
            let topology = self.topology.read().unwrap();
            // The fetched topology is outdated, and we will use the cache.
            let cached_node_topology =
                topology.nodes().context(ClusterNodesNotFound { version })?;
            ClusterNodesResp {
                cluster_topology_version: cached_node_topology.version,
                cluster_nodes: cached_node_topology.nodes,
            }
        };

        Ok(resp)
    }

    async fn open_shard(&self, shard_info: &ShardInfo) -> Result<ShardRef> {
        if let Some(shard) = self.shard_set.get(shard_info.id) {
            let cur_shard_info = shard.shard_info();
            if cur_shard_info.version == shard_info.version {
                info!(
                    "No need to open the exactly same shard again, shard_info:{:?}",
                    shard_info
                );
                return Ok(shard);
            }
            ensure!(
                cur_shard_info.version < shard_info.version,
                OpenShard {
                    shard_id: shard_info.id,
                    msg: format!("open a shard with a smaller version, curr_shard_info:{cur_shard_info:?}, new_shard_info:{shard_info:?}"),
                }
            );
        }

        let req = GetTablesOfShardsRequest {
            shard_ids: vec![shard_info.id],
        };

        let mut resp = self
            .meta_client
            .get_tables_of_shards(req)
            .await
            .box_err()
            .with_context(|| OpenShardWithCause {
                msg: format!("shard_info:{shard_info:?}"),
            })?;

        ensure!(
            resp.tables_by_shard.len() == 1,
            OpenShard {
                shard_id: shard_info.id,
                msg: "expect only one shard tables"
            }
        );

        let tables_of_shard = resp
            .tables_by_shard
            .remove(&shard_info.id)
            .context(OpenShard {
                shard_id: shard_info.id,
                msg: "shard tables are missing from the response",
            })?;

        let shard_id = tables_of_shard.shard_info.id;
        let shard = Arc::new(Shard::new(tables_of_shard));

        info!("Insert shard to shard_set, id:{shard_id}, shard:{shard:?}");
        if let Some(old_shard) = self.shard_set.insert(shard_id, shard.clone()) {
            info!("Remove old shard, id:{shard_id}, old:{old_shard:?}");
        }

        Ok(shard)
    }

    fn shard(&self, shard_id: ShardId) -> Option<ShardRef> {
        self.shard_set.get(shard_id)
    }

    /// Get shard by table name.
    ///
    /// This method is similar to `route_tables`, but it will not send request
    /// to meta server, it only load data from local cache.
    /// If target table is not found in any shards in this cluster, return None.
    /// Otherwise, return the shard where this table is exists.
    fn get_shard_by_table_name(&self, schema_name: &str, table_name: &str) -> Option<ShardRef> {
        let shards = self.shard_set.all_shards();
        shards
            .into_iter()
            .find(|shard| shard.find_table(schema_name, table_name).is_some())
    }

    fn close_shard(&self, shard_id: ShardId) -> Result<ShardRef> {
        info!("Remove shard from shard_set, id:{shard_id}");
        self.shard_set
            .remove(shard_id)
            .with_context(|| ShardNotFound {
                msg: format!("close non-existent shard, shard_id:{shard_id}"),
            })
    }

    fn list_shards(&self) -> Vec<ShardInfo> {
        let shards = self.shard_set.all_shards();

        shards.iter().map(|shard| shard.shard_info()).collect()
    }

    /// Get proper remote compaction node for compaction offload with meta
    /// client.
    async fn get_compaction_node(&self) -> Result<CompactionClientConfig> {
        unimplemented!()
    }

    /// Return a new compaction client.
    async fn compaction_client(&self) -> CompactionClientRef {
        // TODO(leslie): impl better error handling with snafu.
        let config = self
            .get_compaction_node()
            .await
            .expect("fail to get remote compaction node");

        build_compaction_client(config)
            .await
            .expect("fail to build compaction client")
    }

    async fn compact(
        &self,
        req: &ExecuteCompactionTaskRequest,
    ) -> Result<ExecuteCompactionTaskResponse> {
        // TODO(leslie): Execute the compaction task locally when fails to build
        // compaction client.
        let compact_resp = self
            .compaction_client()
            .await
            .execute_compaction_task(req.clone())
            .await
            .context(CompactionClientFailure)?;

        Ok(compact_resp)
    }
}

#[async_trait]
impl Cluster for ClusterImpl {
    /// Type of the server in cluster mode.
    type ClusterType = ClusterType;

    async fn start(&self) -> Result<()> {
        info!("Cluster is starting with config:{:?}", self.config);

        // start the background loop for sending heartbeat.
        self.start_heartbeat_loop();

        info!("Cluster has started");
        Ok(())
    }

    async fn stop(&self) -> Result<()> {
        info!("Cluster is stopping");

        {
            let tx = self.stop_heartbeat_tx.lock().unwrap().take();
            if let Some(tx) = tx {
                let _ = tx.send(()).await;
            }
        }

        {
            let handle = self.heartbeat_handle.lock().unwrap().take();
            if let Some(handle) = handle {
                let _ = handle.await;
            }
        }

        info!("Cluster has stopped");
        Ok(())
    }

    fn cluster_type(&self) -> ClusterType {
        self.config.cluster_type.clone()
    }

    async fn open_shard(&self, shard_info: &ShardInfo) -> Result<ShardRef> {
        self.inner.open_shard(shard_info).await
    }

    fn shard(&self, shard_id: ShardId) -> Option<ShardRef> {
        self.inner.shard(shard_id)
    }

    fn get_table_status(&self, schema_name: &str, table_name: &str) -> Option<TableStatus> {
        self.inner
            .get_shard_by_table_name(schema_name, table_name)
            .map(|shard| TableStatus::from(shard.get_status()))
    }

    async fn close_shard(&self, shard_id: ShardId) -> Result<ShardRef> {
        self.inner.close_shard(shard_id)
    }

    fn list_shards(&self) -> Vec<ShardInfo> {
        self.inner.list_shards()
    }

    async fn route_tables(&self, req: &RouteTablesRequest) -> Result<RouteTablesResponse> {
        self.inner.route_tables(req).await
    }

    async fn fetch_nodes(&self) -> Result<ClusterNodesResp> {
        self.inner.fetch_nodes().await
    }

    fn shard_lock_manager(&self) -> ShardLockManagerRef {
        self.shard_lock_manager.clone()
    }

    async fn compact(
        &self,
        req: &ExecuteCompactionTaskRequest,
    ) -> Result<ExecuteCompactionTaskResponse> {
        ensure!(
            self.cluster_type() == ClusterType::HoraeDB,
            CompactionOffloadNotAllowed {
                cluster_type: self.cluster_type()
            }
        );
        self.inner.compact(req).await
    }
}

/// Build the connect options for accessing etcd cluster.
async fn build_etcd_connect_options(config: &EtcdClientConfig) -> io::Result<ConnectOptions> {
    let connect_options = ConnectOptions::default()
        .with_connect_timeout(config.connect_timeout.0)
        .with_timeout(config.rpc_timeout());

    let tls = &config.tls;
    if tls.enable {
        let server_ca_cert = fs::read(&tls.ca_cert_path).await?;
        let client_cert = fs::read(&tls.client_cert_path).await?;
        let client_key = fs::read(&tls.client_key_path).await?;

        let ca_cert = Certificate::from_pem(server_ca_cert);
        let client_ident = Identity::from_pem(client_cert, client_key);
        let mut tls_options = TlsOptions::new()
            .ca_certificate(ca_cert)
            .identity(client_ident);

        if let Some(domain) = &tls.domain {
            tls_options = tls_options.domain_name(domain);
        }

        Ok(connect_options.with_tls(tls_options))
    } else {
        Ok(connect_options)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_shard_lock_key_prefix() {
        let cases = vec![
            (
                ("/horaedb", "defaultCluster"),
                Some("/horaedb/defaultCluster/shards"),
            ),
            (("", "defaultCluster"), None),
            (("vvv", "defaultCluster"), None),
            (("/x", ""), None),
        ];

        for ((root_path, cluster_name), expected) in cases {
            let actual = ClusterImpl::shard_lock_key_prefix(root_path, cluster_name);
            match expected {
                Some(expected) => assert_eq!(actual.unwrap(), expected),
                None => assert!(actual.is_err()),
            }
        }
    }
}
