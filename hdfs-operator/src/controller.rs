use std::{collections::BTreeMap, fmt::Debug, time::Duration};

use crate::crd::HdfsCluster;
use k8s_openapi::{
    api::{
        apps::v1::{StatefulSet, StatefulSetSpec},
        core::v1::{
            ConfigMap, ConfigMapVolumeSource, Container, ContainerPort, EnvVar,
            PersistentVolumeClaim, PersistentVolumeClaimSpec, PodSpec, PodTemplateSpec,
            ResourceRequirements, Service, ServicePort, ServiceSpec, Volume, VolumeMount,
        },
    },
    apimachinery::pkg::{
        api::resource::Quantity,
        apis::meta::v1::{LabelSelector, OwnerReference},
        util::intstr::IntOrString,
    },
};
use kube::{
    api::{DynamicObject, ObjectMeta, Patch, PatchParams},
    Resource,
};
use kube_runtime::{
    controller::{Context, ReconcilerAction},
    reflector::ObjectRef,
};
use serde::{de::DeserializeOwned, Serialize};
use snafu::{OptionExt, ResultExt, Snafu};

pub struct Ctx {
    pub kube: kube::Client,
}

#[derive(Snafu, Debug)]
#[allow(clippy::enum_variant_names)]
pub enum Error {
    ObjectHasNoNamespace { obj_ref: ObjectRef<DynamicObject> },
    ApplyExternalService { source: kube::Error },
    ApplyPeerService { source: kube::Error },
    ApplyStatefulSet { source: kube::Error },
}

fn controller_reference_to_obj<K: Resource<DynamicType = ()>>(obj: &K) -> OwnerReference {
    OwnerReference {
        api_version: K::api_version(&()).into_owned(),
        kind: K::kind(&()).into_owned(),
        controller: Some(true),
        name: obj.meta().name.clone().unwrap(),
        uid: obj.meta().uid.clone().unwrap(),
        ..OwnerReference::default()
    }
}

fn hadoop_config_xml<I: IntoIterator<Item = (K, V)>, K: AsRef<str>, V: AsRef<str>>(
    kvs: I,
) -> String {
    use std::fmt::Write;
    let mut xml = "<configuration>".to_string();
    for (k, v) in kvs {
        write!(
            xml,
            "<property><name>{}</name><value>{}</value></property>",
            k.as_ref(),
            v.as_ref()
        )
        .unwrap();
    }
    xml.push_str("</configuration>");
    xml
}

fn local_disk_claim(name: &str, size: Quantity) -> PersistentVolumeClaim {
    PersistentVolumeClaim {
        metadata: ObjectMeta {
            name: Some(name.to_string()),
            ..ObjectMeta::default()
        },
        spec: Some(PersistentVolumeClaimSpec {
            access_modes: Some(vec!["ReadWriteOnce".to_string()]),
            resources: Some(ResourceRequirements {
                requests: Some(BTreeMap::from([("storage".to_string(), size)])),
                ..ResourceRequirements::default()
            }),
            ..PersistentVolumeClaimSpec::default()
        }),
        ..PersistentVolumeClaim::default()
    }
}

fn hadoop_container() -> Container {
    Container {
        image: Some("teozkr/hadoop:3.3.1".to_string()),
        env: Some(vec![
            EnvVar {
                name: "HADOOP_HOME".to_string(),
                value: Some("/opt/hadoop".to_string()),
                ..EnvVar::default()
            },
            EnvVar {
                name: "HADOOP_CONF_DIR".to_string(),
                value: Some("/config".to_string()),
                ..EnvVar::default()
            },
        ]),
        volume_mounts: Some(vec![
            VolumeMount {
                mount_path: "/data".to_string(),
                name: "data".to_string(),
                ..VolumeMount::default()
            },
            VolumeMount {
                mount_path: "/config".to_string(),
                name: "config".to_string(),
                ..VolumeMount::default()
            },
        ]),
        ..Container::default()
    }
}

async fn apply_owned<K>(kube: &kube::Client, obj: K) -> kube::Result<K>
where
    K: Resource<DynamicType = ()> + Serialize + DeserializeOwned + Clone + Debug,
{
    let api = if let Some(ns) = &obj.meta().namespace {
        kube::Api::<K>::namespaced(kube.clone(), ns)
    } else {
        kube::Api::<K>::all(kube.clone())
    };
    api.patch(
        &obj.meta().name.clone().unwrap(),
        &PatchParams {
            force: true,
            field_manager: Some("hdfs.stackable.tech/hdfscluster".to_string()),
            ..PatchParams::default()
        },
        &Patch::Apply(obj),
    )
    .await
}

pub async fn reconcile_hdfs(
    hdfs: HdfsCluster,
    ctx: Context<Ctx>,
) -> Result<ReconcilerAction, Error> {
    let ns = hdfs
        .metadata
        .namespace
        .as_deref()
        .with_context(|| ObjectHasNoNamespace {
            obj_ref: ObjectRef::from_obj(&hdfs).erase(),
        })?;
    let kube = ctx.get_ref().kube.clone();

    let name = hdfs.metadata.name.clone().unwrap();
    let hdfs_owner_ref = controller_reference_to_obj(&hdfs);
    let config_name = format!("{}-config", name);
    let pod_labels = BTreeMap::from([("app".to_string(), "hdfs".to_string())]);

    let nameservice_id = name.clone();
    let namenode_name = format!("{}-namenode", name);
    let namenode_fqdn = format!("{}.{}.svc.cluster.local", namenode_name, ns);
    let namenode_pod_fqdn = |i: i32| format!("{}-{}.{}", namenode_name, i, namenode_fqdn);
    let mut namenode_pod_labels = pod_labels.clone();
    namenode_pod_labels.extend([("role".to_string(), "namenode".to_string())]);

    let datanode_name = format!("{}-datanode", name);
    let mut datanode_pod_labels = pod_labels.clone();
    datanode_pod_labels.extend([("role".to_string(), "datanode".to_string())]);

    let journalnode_name = format!("{}-journalnode", name);
    let journalnode_fqdn = format!("{}.{}.svc.cluster.local", journalnode_name, ns);
    let journalnode_pod_fqdn = |i: i32| format!("{}-{}.{}", journalnode_name, i, journalnode_fqdn);
    let mut journalnode_pod_labels = pod_labels.clone();
    journalnode_pod_labels.extend([("role".to_string(), "journalnode".to_string())]);

    let hdfs_site_config = [
        ("dfs.namenode.name.dir".to_string(), "/data".to_string()),
        ("dfs.datanode.data.dir".to_string(), "/data".to_string()),
        ("dfs.journalnode.edits.dir".to_string(), "/data".to_string()),
        ("dfs.nameservices".to_string(), nameservice_id.clone()),
        (
            format!("dfs.ha.namenodes.{}", nameservice_id),
            (0..hdfs.spec.namenode_replicas.unwrap_or(1))
                .map(|i| format!("name-{}", i))
                .collect::<Vec<_>>()
                .join(", "),
        ),
        (
            "dfs.namenode.shared.edits.dir".to_string(),
            format!(
                "qjournal://{}/{}",
                (0..hdfs.spec.journalnode_replicas.unwrap_or(1))
                    .map(journalnode_pod_fqdn)
                    .collect::<Vec<_>>()
                    .join(";"),
                nameservice_id
            ),
        ),
        (
            format!("dfs.client.failover.proxy.provider.{}", nameservice_id),
            "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider".to_string(),
        ),
        (
            "dfs.ha.fencing.methods".to_string(),
            "shell(/bin/true)".to_string(),
        ),
        (
            "dfs.ha.nn.not-become-active-in-safemode".to_string(),
            "true".to_string(),
        ),
        (
            "dfs.ha.automatic-failover.enabled".to_string(),
            "true".to_string(),
        ),
        ("ha.zookeeper.quorum".to_string(), "zkc:2181".to_string()),
    ]
    .into_iter()
    .chain((0..hdfs.spec.namenode_replicas.unwrap_or(1)).flat_map(|i| {
        [
            (
                format!("dfs.namenode.rpc-address.{}.name-{}", nameservice_id, i),
                format!("{}:8020", namenode_pod_fqdn(i)),
            ),
            (
                format!("dfs.namenode.http-address.{}.name-{}", nameservice_id, i),
                format!("{}:9870", namenode_pod_fqdn(i)),
            ),
        ]
    }));
    apply_owned(
        &kube,
        ConfigMap {
            metadata: ObjectMeta {
                owner_references: Some(vec![hdfs_owner_ref.clone()]),
                name: Some(config_name.clone()),
                namespace: Some(ns.to_string()),
                ..ObjectMeta::default()
            },
            data: Some(BTreeMap::from([
                (
                    "core-site.xml".to_string(),
                    hadoop_config_xml([("fs.defaultFS", format!("hdfs://{}/", name))]),
                ),
                (
                    "hdfs-site.xml".to_string(),
                    hadoop_config_xml(hdfs_site_config),
                ),
            ])),
            ..ConfigMap::default()
        },
    )
    .await
    .unwrap();
    apply_owned(
        &kube,
        Service {
            metadata: ObjectMeta {
                owner_references: Some(vec![hdfs_owner_ref.clone()]),
                name: Some(journalnode_name.clone()),
                namespace: Some(ns.to_string()),
                ..ObjectMeta::default()
            },
            spec: Some(ServiceSpec {
                ports: Some(vec![ServicePort {
                    name: Some("ipc".to_string()),
                    port: 8485,
                    protocol: Some("TCP".to_string()),
                    ..ServicePort::default()
                }]),
                selector: Some(journalnode_pod_labels.clone()),
                cluster_ip: Some("None".to_string()),
                ..ServiceSpec::default()
            }),
            status: None,
        },
    )
    .await
    .context(ApplyPeerService)?;
    let journalnode_pod_template = PodTemplateSpec {
        metadata: Some(ObjectMeta {
            labels: Some(journalnode_pod_labels.clone()),
            ..ObjectMeta::default()
        }),
        spec: Some(PodSpec {
            containers: vec![Container {
                name: "journalnode".to_string(),
                args: Some(vec![
                    "/opt/hadoop/bin/hdfs".to_string(),
                    "journalnode".to_string(),
                ]),
                ports: Some(vec![ContainerPort {
                    name: Some("ipc".to_string()),
                    container_port: 8485,
                    protocol: Some("TCP".to_string()),
                    ..ContainerPort::default()
                }]),
                ..hadoop_container()
            }],
            volumes: Some(vec![Volume {
                name: "config".to_string(),
                config_map: Some(ConfigMapVolumeSource {
                    name: Some(format!("{}-config", name)),
                    ..ConfigMapVolumeSource::default()
                }),
                ..Volume::default()
            }]),
            host_network: Some(true),
            dns_policy: Some("ClusterFirstWithHostNet".to_string()),
            ..PodSpec::default()
        }),
    };
    apply_owned(
        &kube,
        StatefulSet {
            metadata: ObjectMeta {
                owner_references: Some(vec![hdfs_owner_ref.clone()]),
                name: Some(journalnode_name.clone()),
                namespace: Some(ns.to_string()),
                ..ObjectMeta::default()
            },
            spec: Some(StatefulSetSpec {
                pod_management_policy: Some("Parallel".to_string()),
                replicas: hdfs.spec.journalnode_replicas,
                selector: LabelSelector {
                    match_labels: Some(journalnode_pod_labels.clone()),
                    ..LabelSelector::default()
                },
                service_name: journalnode_name.clone(),
                template: journalnode_pod_template,
                volume_claim_templates: Some(vec![local_disk_claim(
                    "data",
                    Quantity("1Gi".to_string()),
                )]),
                ..StatefulSetSpec::default()
            }),
            status: None,
        },
    )
    .await
    .context(ApplyStatefulSet)?;
    apply_owned(
        &kube,
        Service {
            metadata: ObjectMeta {
                owner_references: Some(vec![hdfs_owner_ref.clone()]),
                name: Some(namenode_name.clone()),
                namespace: Some(ns.to_string()),
                ..ObjectMeta::default()
            },
            spec: Some(ServiceSpec {
                ports: Some(vec![
                    ServicePort {
                        name: Some("ipc".to_string()),
                        port: 8020,
                        protocol: Some("TCP".to_string()),
                        ..ServicePort::default()
                    },
                    ServicePort {
                        name: Some("http".to_string()),
                        port: 80,
                        target_port: Some(IntOrString::String("http".to_string())),
                        protocol: Some("TCP".to_string()),
                        ..ServicePort::default()
                    },
                ]),
                selector: Some(namenode_pod_labels.clone()),
                cluster_ip: Some("None".to_string()),
                publish_not_ready_addresses: Some(true),
                ..ServiceSpec::default()
            }),
            status: None,
        },
    )
    .await
    .context(ApplyPeerService)?;
    let namenode_pod_template = PodTemplateSpec {
        metadata: Some(ObjectMeta {
            labels: Some(namenode_pod_labels.clone()),
            ..ObjectMeta::default()
        }),
        spec: Some(PodSpec {
            init_containers: Some(vec![Container {
                name: "format-namenode".to_string(),
                args: Some(vec![
                    "sh".to_string(),
                    "-c".to_string(),
                    "/opt/hadoop/bin/hdfs namenode -bootstrapStandby -nonInteractive \
                     || /opt/hadoop/bin/hdfs namenode -format -noninteractive \
                     || true
                     /opt/hadoop/bin/hdfs zkfc -formatZK -nonInteractive || true"
                        .to_string(),
                ]),
                ..hadoop_container()
            }]),
            containers: vec![
                Container {
                    name: "namenode".to_string(),
                    args: Some(vec![
                        "/opt/hadoop/bin/hdfs".to_string(),
                        "namenode".to_string(),
                    ]),
                    ports: Some(vec![
                        ContainerPort {
                            name: Some("ipc".to_string()),
                            container_port: 8020,
                            protocol: Some("TCP".to_string()),
                            ..ContainerPort::default()
                        },
                        ContainerPort {
                            name: Some("http".to_string()),
                            container_port: 9870,
                            protocol: Some("TCP".to_string()),
                            ..ContainerPort::default()
                        },
                    ]),
                    ..hadoop_container()
                },
                Container {
                    name: "zkfc".to_string(),
                    args: Some(vec!["/opt/hadoop/bin/hdfs".to_string(), "zkfc".to_string()]),
                    ..hadoop_container()
                },
            ],
            volumes: Some(vec![Volume {
                name: "config".to_string(),
                config_map: Some(ConfigMapVolumeSource {
                    name: Some(format!("{}-config", name)),
                    ..ConfigMapVolumeSource::default()
                }),
                ..Volume::default()
            }]),
            host_network: Some(true),
            dns_policy: Some("ClusterFirstWithHostNet".to_string()),
            ..PodSpec::default()
        }),
    };
    apply_owned(
        &kube,
        StatefulSet {
            metadata: ObjectMeta {
                owner_references: Some(vec![hdfs_owner_ref.clone()]),
                name: Some(namenode_name.clone()),
                namespace: Some(ns.to_string()),
                ..ObjectMeta::default()
            },
            spec: Some(StatefulSetSpec {
                pod_management_policy: Some("Parallel".to_string()),
                replicas: hdfs.spec.namenode_replicas,
                selector: LabelSelector {
                    match_labels: Some(namenode_pod_labels.clone()),
                    ..LabelSelector::default()
                },
                service_name: namenode_name.clone(),
                template: namenode_pod_template,
                volume_claim_templates: Some(vec![local_disk_claim(
                    "data",
                    Quantity("1Gi".to_string()),
                )]),
                // volume_claim_templates: todo!(),
                ..StatefulSetSpec::default()
            }),
            status: None,
        },
    )
    .await
    .context(ApplyStatefulSet)?;
    apply_owned(
        &kube,
        Service {
            metadata: ObjectMeta {
                owner_references: Some(vec![hdfs_owner_ref.clone()]),
                name: Some(datanode_name.clone()),
                namespace: Some(ns.to_string()),
                ..ObjectMeta::default()
            },
            spec: Some(ServiceSpec {
                ports: Some(vec![
                    ServicePort {
                        name: Some("ipc".to_string()),
                        port: 9867,
                        protocol: Some("TCP".to_string()),
                        ..ServicePort::default()
                    },
                    ServicePort {
                        name: Some("http".to_string()),
                        port: 80,
                        target_port: Some(IntOrString::String("http".to_string())),
                        protocol: Some("TCP".to_string()),
                        ..ServicePort::default()
                    },
                ]),
                selector: Some(datanode_pod_labels.clone()),
                cluster_ip: Some("None".to_string()),
                ..ServiceSpec::default()
            }),
            status: None,
        },
    )
    .await
    .context(ApplyPeerService)?;
    let datanode_pod_template = PodTemplateSpec {
        metadata: Some(ObjectMeta {
            labels: Some(datanode_pod_labels.clone()),
            ..ObjectMeta::default()
        }),
        spec: Some(PodSpec {
            // init_containers: Some(vec![Container {
            //     name: "format-namenode".to_string(),
            //     image: Some("teozkr/hadoop:3.3.1".to_string()),
            //     args: Some(vec![
            //         "sh".to_string(),
            //         "-c".to_string(),
            //         "stat /data/current || /opt/hadoop/bin/hdfs namenode -format -noninteractive"
            //             .to_string(),
            //     ]),
            //     env: Some(vec![
            //         EnvVar {
            //             name: "HADOOP_HOME".to_string(),
            //             value: Some("/opt/hadoop".to_string()),
            //             ..EnvVar::default()
            //         },
            //         EnvVar {
            //             name: "HADOOP_CONF_DIR".to_string(),
            //             value: Some("/config".to_string()),
            //             ..EnvVar::default()
            //         },
            //     ]),
            //     volume_mounts: Some(vec![
            //         VolumeMount {
            //             mount_path: "/data".to_string(),
            //             name: "data".to_string(),
            //             ..VolumeMount::default()
            //         },
            //         VolumeMount {
            //             mount_path: "/config".to_string(),
            //             name: "config".to_string(),
            //             ..VolumeMount::default()
            //         },
            //     ]),
            //     ..Container::default()
            // }]),
            containers: vec![Container {
                name: "datanode".to_string(),
                image: Some("teozkr/hadoop:3.3.1".to_string()),
                args: Some(vec![
                    "/opt/hadoop/bin/hdfs".to_string(),
                    "datanode".to_string(),
                ]),
                env: Some(vec![
                    EnvVar {
                        name: "HADOOP_HOME".to_string(),
                        value: Some("/opt/hadoop".to_string()),
                        ..EnvVar::default()
                    },
                    EnvVar {
                        name: "HADOOP_CONF_DIR".to_string(),
                        value: Some("/config".to_string()),
                        ..EnvVar::default()
                    },
                ]),
                ports: Some(vec![
                    ContainerPort {
                        name: Some("ipc".to_string()),
                        container_port: 9867,
                        protocol: Some("TCP".to_string()),
                        ..ContainerPort::default()
                    },
                    ContainerPort {
                        name: Some("data".to_string()),
                        container_port: 9866,
                        protocol: Some("TCP".to_string()),
                        ..ContainerPort::default()
                    },
                    ContainerPort {
                        name: Some("http".to_string()),
                        container_port: 9864,
                        protocol: Some("TCP".to_string()),
                        ..ContainerPort::default()
                    },
                ]),
                volume_mounts: Some(vec![
                    VolumeMount {
                        mount_path: "/data".to_string(),
                        name: "data".to_string(),
                        ..VolumeMount::default()
                    },
                    VolumeMount {
                        mount_path: "/config".to_string(),
                        name: "config".to_string(),
                        ..VolumeMount::default()
                    },
                ]),
                ..Container::default()
            }],
            volumes: Some(vec![Volume {
                name: "config".to_string(),
                config_map: Some(ConfigMapVolumeSource {
                    name: Some(format!("{}-config", name)),
                    ..ConfigMapVolumeSource::default()
                }),
                ..Volume::default()
            }]),
            host_network: Some(true),
            dns_policy: Some("ClusterFirstWithHostNet".to_string()),
            ..PodSpec::default()
        }),
    };
    apply_owned(
        &kube,
        StatefulSet {
            metadata: ObjectMeta {
                owner_references: Some(vec![hdfs_owner_ref.clone()]),
                name: Some(datanode_name.clone()),
                namespace: Some(ns.to_string()),
                ..ObjectMeta::default()
            },
            spec: Some(StatefulSetSpec {
                pod_management_policy: Some("Parallel".to_string()),
                replicas: hdfs.spec.datanode_replicas,
                selector: LabelSelector {
                    match_labels: Some(datanode_pod_labels.clone()),
                    ..LabelSelector::default()
                },
                service_name: datanode_name.clone(),
                template: datanode_pod_template,
                volume_claim_templates: Some(vec![local_disk_claim(
                    "data",
                    Quantity("1Gi".to_string()),
                )]),
                // volume_claim_templates: todo!(),
                ..StatefulSetSpec::default()
            }),
            status: None,
        },
    )
    .await
    .context(ApplyStatefulSet)?;

    Ok(ReconcilerAction {
        requeue_after: None,
    })
}

pub fn error_policy(_error: &Error, _ctx: Context<Ctx>) -> ReconcilerAction {
    ReconcilerAction {
        requeue_after: Some(Duration::from_secs(5)),
    }
}
