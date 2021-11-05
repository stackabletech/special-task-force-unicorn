use std::fmt::Display;

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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namenode_znode_config_map: Option<String>,
    #[serde(default)]
    pub kerberos: KerberosConfig,
}

#[derive(Clone, Debug, Default, Deserialize, JsonSchema, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct KerberosConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub realm: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kdc: Option<String>,
}

impl Display for KerberosConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "[libdefaults]")?;
        if let Some(realm) = &self.realm {
            writeln!(f, "default_realm = {}", realm)?;
        }
        writeln!(f, "[realms]")?;
        if let Some(realm) = &self.realm {
            writeln!(f, "{} = {{", realm)?;
            if let Some(kdc) = &self.kdc {
                writeln!(f, "kdc = {}", kdc)?;
            }
            writeln!(f, "}}")?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Default, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HdfsClusterStatus {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conditions: Option<Vec<Condition>>,
}
