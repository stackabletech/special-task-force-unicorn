use std::{collections::BTreeMap, time::Duration};

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
    let stses = kube::Api::<StatefulSet>::namespaced(ctx.get_ref().kube.clone(), ns);
    let svcs = kube::Api::<Service>::namespaced(ctx.get_ref().kube.clone(), ns);
    let cms = kube::Api::<ConfigMap>::namespaced(ctx.get_ref().kube.clone(), ns);

    let patch_params = PatchParams {
        force: true,
        field_manager: Some("hdfs.stackable.tech/hdfscluster".to_string()),
        ..PatchParams::default()
    };

    let name = hdfs.metadata.name.clone().unwrap();
    let hdfs_owner_ref = controller_reference_to_obj(&hdfs);
    let config_name = format!("{}-config", name);
    let pod_labels = BTreeMap::from([("app".to_string(), "hdfs".to_string())]);

    let namenode_name = format!("{}-namenode", name);
    let mut namenode_pod_labels = pod_labels.clone();
    namenode_pod_labels.extend([("role".to_string(), "namenode".to_string())]);

    let datanode_name = format!("{}-datanode", name);
    let mut datanode_pod_labels = pod_labels.clone();
    datanode_pod_labels.extend([("role".to_string(), "datanode".to_string())]);

    svcs.patch(
        &namenode_name,
        &patch_params,
        &Patch::Apply(Service {
            metadata: ObjectMeta {
                owner_references: Some(vec![hdfs_owner_ref.clone()]),
                name: Some(namenode_name.clone()),
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
                ..ServiceSpec::default()
            }),
            status: None,
        }),
    )
    .await
    .context(ApplyPeerService)?;
    cms.patch(
        &config_name,
        &patch_params,
        &Patch::Apply(ConfigMap {
            metadata: ObjectMeta {
                owner_references: Some(vec![hdfs_owner_ref.clone()]),
                name: Some(config_name.clone()),
                ..ObjectMeta::default()
            },
            data: Some(BTreeMap::from([
                (
                    "core-site.xml".to_string(),
                    hadoop_config_xml([(
                        "fs.defaultFS",
                        format!("hdfs://{}.{}.svc.cluster.local/", namenode_name, ns),
                    )]),
                ),
                (
                    "hdfs-site.xml".to_string(),
                    hadoop_config_xml([
                        ("dfs.namenode.name.dir", "/data".to_string()),
                        ("dfs.datanode.data.dir", "/data".to_string()),
                    ]),
                ),
            ])),
            ..ConfigMap::default()
        }),
    )
    .await
    .unwrap();
    let namenode_pod_template = PodTemplateSpec {
        metadata: Some(ObjectMeta {
            labels: Some(namenode_pod_labels.clone()),
            ..ObjectMeta::default()
        }),
        spec: Some(PodSpec {
            init_containers: Some(vec![Container {
                name: "format-namenode".to_string(),
                image: Some("teozkr/hadoop:3.3.1".to_string()),
                args: Some(vec![
                    "sh".to_string(),
                    "-c".to_string(),
                    "stat /data/current || /opt/hadoop/bin/hdfs namenode -format -noninteractive"
                        .to_string(),
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
            }]),
            containers: vec![Container {
                name: "namenode".to_string(),
                image: Some("teozkr/hadoop:3.3.1".to_string()),
                args: Some(vec![
                    "/opt/hadoop/bin/hdfs".to_string(),
                    "namenode".to_string(),
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
    stses
        .patch(
            &namenode_name,
            &patch_params,
            &Patch::Apply(StatefulSet {
                metadata: ObjectMeta {
                    owner_references: Some(vec![hdfs_owner_ref.clone()]),
                    name: Some(namenode_name.clone()),
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
                    volume_claim_templates: Some(vec![PersistentVolumeClaim {
                        metadata: ObjectMeta {
                            name: Some("data".to_string()),
                            ..ObjectMeta::default()
                        },
                        spec: Some(PersistentVolumeClaimSpec {
                            access_modes: Some(vec!["ReadWriteOnce".to_string()]),
                            resources: Some(ResourceRequirements {
                                requests: Some(BTreeMap::from([(
                                    "storage".to_string(),
                                    Quantity("1Gi".to_string()),
                                )])),
                                ..ResourceRequirements::default()
                            }),
                            ..PersistentVolumeClaimSpec::default()
                        }),
                        ..PersistentVolumeClaim::default()
                    }]),
                    // volume_claim_templates: todo!(),
                    ..StatefulSetSpec::default()
                }),
                status: None,
            }),
        )
        .await
        .context(ApplyStatefulSet)?;
    svcs.patch(
        &datanode_name,
        &patch_params,
        &Patch::Apply(Service {
            metadata: ObjectMeta {
                owner_references: Some(vec![hdfs_owner_ref.clone()]),
                name: Some(datanode_name.clone()),
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
        }),
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
    stses
        .patch(
            &datanode_name,
            &patch_params,
            &Patch::Apply(StatefulSet {
                metadata: ObjectMeta {
                    owner_references: Some(vec![hdfs_owner_ref.clone()]),
                    name: Some(datanode_name.clone()),
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
                    volume_claim_templates: Some(vec![PersistentVolumeClaim {
                        metadata: ObjectMeta {
                            name: Some("data".to_string()),
                            ..ObjectMeta::default()
                        },
                        spec: Some(PersistentVolumeClaimSpec {
                            access_modes: Some(vec!["ReadWriteOnce".to_string()]),
                            resources: Some(ResourceRequirements {
                                requests: Some(BTreeMap::from([(
                                    "storage".to_string(),
                                    Quantity("1Gi".to_string()),
                                )])),
                                ..ResourceRequirements::default()
                            }),
                            ..PersistentVolumeClaimSpec::default()
                        }),
                        ..PersistentVolumeClaim::default()
                    }]),
                    // volume_claim_templates: todo!(),
                    ..StatefulSetSpec::default()
                }),
                status: None,
            }),
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
