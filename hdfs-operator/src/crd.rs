use k8s_openapi::apimachinery::pkg::apis::meta::v1::Condition;
use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Clone, CustomResource, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[kube(
    group = "hdfs.stackable.tech",
    version = "v1alpha1",
    kind = "HdfsCluster",
    plural = "hdfsclusters",
    shortname = "hdfs",
    namespaced
)]
#[kube(status = "HdfsClusterStatus")]
#[serde(rename_all = "camelCase")]
pub struct HdfsClusterSpec {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namenode_replicas: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub datanode_replicas: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub journalnode_replicas: Option<i32>,
}

#[derive(Clone, Debug, Default, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HdfsClusterStatus {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conditions: Option<Vec<Condition>>,
}
