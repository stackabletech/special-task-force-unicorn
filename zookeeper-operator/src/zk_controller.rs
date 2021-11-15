//! Ensures that `Pod`s are configured and running for each [`ZookeeperCluster`]

use std::{collections::BTreeMap, time::Duration};

use crate::{
    crd::ZookeeperCluster,
    utils::{apply_owned, controller_reference_to_obj},
};
use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::{
    builder::{ConfigMapBuilder, ContainerBuilder},
    k8s_openapi::{
        api::{
            apps::v1::{StatefulSet, StatefulSetSpec},
            core::v1::{
                ConfigMapVolumeSource, EnvVar, EnvVarSource, ExecAction, ObjectFieldSelector,
                PersistentVolumeClaim, PersistentVolumeClaimSpec, PodSpec, PodTemplateSpec, Probe,
                ResourceRequirements, Service, ServicePort, ServiceSpec, Volume,
            },
        },
        apimachinery::pkg::{api::resource::Quantity, apis::meta::v1::LabelSelector},
    },
    kube::{
        self,
        api::ObjectMeta,
        runtime::{
            controller::{Context, ReconcilerAction},
            reflector::ObjectRef,
        },
    },
    labels::get_recommended_labels,
};

const FIELD_MANAGER: &str = "zookeeper.stackable.tech/zookeepercluster";

pub struct Ctx {
    pub kube: kube::Client,
}

#[derive(Snafu, Debug)]
#[allow(clippy::enum_variant_names)]
pub enum Error {
    #[snafu(display("object {} has no namespace", obj_ref))]
    ObjectHasNoNamespace {
        obj_ref: ObjectRef<ZookeeperCluster>,
    },
    #[snafu(display("failed to calculate global service name for {}", obj_ref))]
    GlobalServiceNameNotFound {
        obj_ref: ObjectRef<ZookeeperCluster>,
    },
    #[snafu(display("failed to calculate service name for role {} of {}", role, obj_ref))]
    RoleServiceNameNotFound {
        obj_ref: ObjectRef<ZookeeperCluster>,
        role: String,
    },
    #[snafu(display("failed to apply global Service for {}", zk))]
    ApplyGlobalService {
        source: kube::Error,
        zk: ObjectRef<ZookeeperCluster>,
    },
    #[snafu(display("failed to apply Service for role {} of {}", role, zk))]
    ApplyRoleService {
        source: kube::Error,
        zk: ObjectRef<ZookeeperCluster>,
        role: String,
    },
    #[snafu(display("failed to apply ConfigMap for role {} of {}", role, zk))]
    ApplyRoleConfig {
        source: kube::Error,
        zk: ObjectRef<ZookeeperCluster>,
        role: String,
    },
    #[snafu(display("failed to apply StatefulSet for role {} of {}", role, zk))]
    ApplyStatefulSet {
        source: kube::Error,
        zk: ObjectRef<ZookeeperCluster>,
        role: String,
    },
}

pub async fn reconcile_zk(
    zk: ZookeeperCluster,
    ctx: Context<Ctx>,
) -> Result<ReconcilerAction, Error> {
    let zk_ref = ObjectRef::from_obj(&zk);
    let ns = zk
        .metadata
        .namespace
        .as_deref()
        .with_context(|| ObjectHasNoNamespace {
            obj_ref: zk_ref.clone(),
        })?;
    let kube = ctx.get_ref().kube.clone();

    let global_svc_name = zk
        .global_service_name()
        .with_context(|| RoleServiceNameNotFound {
            obj_ref: zk_ref.clone(),
            role: "servers",
        })?;
    let role_svc_servers_name =
        zk.server_role_service_name()
            .with_context(|| RoleServiceNameNotFound {
                obj_ref: zk_ref.clone(),
                role: "servers",
            })?;
    let zk_owner_ref = controller_reference_to_obj(&zk);
    let pod_labels = get_recommended_labels(&zk, "zookeeper", "3.7.0", "servers", "servers");
    apply_owned(
        &kube,
        FIELD_MANAGER,
        &Service {
            metadata: ObjectMeta {
                name: Some(global_svc_name.clone()),
                namespace: Some(ns.to_string()),
                owner_references: Some(vec![zk_owner_ref.clone()]),
                ..ObjectMeta::default()
            },
            spec: Some(ServiceSpec {
                ports: Some(vec![ServicePort {
                    name: Some("zk".to_string()),
                    port: 2181,
                    protocol: Some("TCP".to_string()),
                    ..ServicePort::default()
                }]),
                selector: Some(pod_labels.clone()),
                type_: Some("NodePort".to_string()),
                ..ServiceSpec::default()
            }),
            status: None,
        },
    )
    .await
    .with_context(|| ApplyGlobalService { zk: zk_ref.clone() })?;
    apply_owned(
        &kube,
        FIELD_MANAGER,
        &Service {
            metadata: ObjectMeta {
                name: Some(role_svc_servers_name.clone()),
                namespace: Some(ns.to_string()),
                owner_references: Some(vec![zk_owner_ref.clone()]),
                ..ObjectMeta::default()
            },
            spec: Some(ServiceSpec {
                cluster_ip: Some("None".to_string()),
                ports: Some(vec![ServicePort {
                    name: Some("zk".to_string()),
                    port: 2181,
                    protocol: Some("TCP".to_string()),
                    ..ServicePort::default()
                }]),
                selector: Some(pod_labels.clone()),
                publish_not_ready_addresses: Some(true),
                ..ServiceSpec::default()
            }),
            status: None,
        },
    )
    .await
    .with_context(|| ApplyRoleService {
        role: "servers",
        zk: zk_ref.clone(),
    })?;
    apply_owned(
        &kube,
        FIELD_MANAGER,
        &ConfigMapBuilder::new()
            .metadata(ObjectMeta {
                name: Some(role_svc_servers_name.clone()),
                namespace: Some(ns.to_string()),
                owner_references: Some(vec![zk_owner_ref.clone()]),
                ..ObjectMeta::default()
            })
            .add_data(
                "zoo.cfg",
                format!(
                    "
tickTime=2000
initLimit=10
syncLimit=5
dataDir=/data
clientPort=2181
{}
",
                    zk.pods()
                        .unwrap()
                        .into_iter()
                        .map(|pod| format!(
                            "server.{}={}:2888:3888;2181",
                            pod.zookeeper_id,
                            pod.fqdn()
                        ))
                        .collect::<Vec<_>>()
                        .join("\n")
                ),
            )
            .build()
            .unwrap(),
    )
    .await
    .with_context(|| ApplyRoleConfig {
        role: "servers",
        zk: zk_ref.clone(),
    })?;
    let container_decide_myid = ContainerBuilder::new("decide-myid")
        .image("alpine")
        .args(vec![
            "sh".to_string(),
            "-c".to_string(),
            "expr 1 + $(echo $POD_NAME | sed 's/.*-//') > /data/myid".to_string(),
        ])
        .add_env_vars(vec![EnvVar {
            name: "POD_NAME".to_string(),
            value_from: Some(EnvVarSource {
                field_ref: Some(ObjectFieldSelector {
                    api_version: Some("v1".to_string()),
                    field_path: "metadata.name".to_string(),
                }),
                ..EnvVarSource::default()
            }),
            ..EnvVar::default()
        }])
        .add_volume_mount("data", "/data")
        .build();
    let mut container_zk = ContainerBuilder::new("zookeeper")
        .image("docker.stackable.tech/stackable/zookeeper:3.5.8-stackable0")
        .args(vec![
            "bin/zkServer.sh".to_string(),
            "start-foreground".to_string(),
            "/config/zoo.cfg".to_string(),
        ])
        .add_container_port("zk", 2181)
        .add_container_port("zk-leader", 2888)
        .add_container_port("zk-election", 3888)
        .add_volume_mount("data", "/data")
        .add_volume_mount("config", "/config")
        .build();
    container_zk.readiness_probe = Some(Probe {
        exec: Some(ExecAction {
            command: Some(vec![
                "sh".to_string(),
                "-c".to_string(),
                "exec 3<>/dev/tcp/localhost/2181 && echo srvr >&3 && grep '^Mode: ' <&3"
                    .to_string(),
            ]),
        }),
        period_seconds: Some(1),
        ..Probe::default()
    });
    apply_owned(
        &kube,
        FIELD_MANAGER,
        &StatefulSet {
            metadata: ObjectMeta {
                name: Some(role_svc_servers_name.clone()),
                namespace: Some(ns.to_string()),
                owner_references: Some(vec![zk_owner_ref.clone()]),
                ..ObjectMeta::default()
            },
            spec: Some(StatefulSetSpec {
                pod_management_policy: Some("Parallel".to_string()),
                replicas: if zk.spec.stopped.unwrap_or(false) {
                    Some(0)
                } else {
                    zk.spec.replicas
                },
                selector: LabelSelector {
                    match_labels: Some(pod_labels.clone()),
                    ..LabelSelector::default()
                },
                service_name: role_svc_servers_name.clone(),
                template: PodTemplateSpec {
                    metadata: Some(ObjectMeta {
                        labels: Some(pod_labels.clone()),
                        ..ObjectMeta::default()
                    }),
                    spec: Some(PodSpec {
                        init_containers: Some(vec![container_decide_myid]),
                        containers: vec![container_zk],
                        volumes: Some(vec![Volume {
                            name: "config".to_string(),
                            config_map: Some(ConfigMapVolumeSource {
                                name: Some(role_svc_servers_name.clone()),
                                ..ConfigMapVolumeSource::default()
                            }),
                            ..Volume::default()
                        }]),
                        ..PodSpec::default()
                    }),
                },
                volume_claim_templates: Some(vec![PersistentVolumeClaim {
                    metadata: ObjectMeta {
                        name: Some("data".to_string()),
                        ..ObjectMeta::default()
                    },
                    spec: Some(PersistentVolumeClaimSpec {
                        access_modes: Some(vec!["ReadWriteOnce".to_string()]),
                        resources: Some(ResourceRequirements {
                            requests: Some({
                                let mut map = BTreeMap::new();
                                map.insert("storage".to_string(), Quantity("1Gi".to_string()));
                                map
                            }),
                            ..ResourceRequirements::default()
                        }),
                        ..PersistentVolumeClaimSpec::default()
                    }),
                    ..PersistentVolumeClaim::default()
                }]),
                ..StatefulSetSpec::default()
            }),
            status: None,
        },
    )
    .await
    .with_context(|| ApplyStatefulSet {
        role: "servers",
        zk: zk_ref.clone(),
    })?;

    Ok(ReconcilerAction {
        requeue_after: None,
    })
}

pub fn error_policy(_error: &Error, _ctx: Context<Ctx>) -> ReconcilerAction {
    ReconcilerAction {
        requeue_after: Some(Duration::from_secs(5)),
    }
}
