mod controller;
mod crd;

use crd::HdfsCluster;
use futures::StreamExt;
use k8s_openapi::api::{apps::v1::StatefulSet, core::v1::Service};
use kube::{api::ListParams, CustomResourceExt};
use kube_runtime::{controller::Context, Controller};
use structopt::StructOpt;

#[derive(StructOpt)]
struct Opts {
    #[structopt(subcommand)]
    cmd: Cmd,
}

#[derive(StructOpt)]
enum Cmd {
    /// Print CRD objects
    Crd,
    Run,
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    tracing_subscriber::fmt().init();

    let opts = Opts::from_args();
    match opts.cmd {
        Cmd::Crd => println!("{}", serde_yaml::to_string(&HdfsCluster::crd())?),
        Cmd::Run => {
            let kube = kube::Client::try_default().await?;
            let zks = kube::Api::<HdfsCluster>::all(kube.clone());
            Controller::new(zks, ListParams::default())
                .owns(
                    kube::Api::<Service>::all(kube.clone()),
                    ListParams::default(),
                )
                .owns(
                    kube::Api::<StatefulSet>::all(kube.clone()),
                    ListParams::default(),
                )
                .run(
                    controller::reconcile_hdfs,
                    controller::error_policy,
                    Context::new(controller::Ctx { kube }),
                )
                .for_each(|res| async {
                    match res {
                        Ok((obj, _)) => tracing::info!(object = %obj, "Reconciled object"),
                        Err(err) => {
                            tracing::error!(
                                error = &err as &dyn std::error::Error,
                                "Failed to reconcile object",
                            )
                        }
                    }
                })
                .await;
        }
    }
    Ok(())
}
