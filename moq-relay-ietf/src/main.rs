use clap::Parser;

mod api;
mod consumer;
mod local;
mod producer;
mod relay;
mod remote;
mod session;
mod web;

pub use api::*;
pub use consumer::*;
pub use local::*;
pub use producer::*;
pub use relay::*;
pub use remote::*;
pub use session::*;
pub use web::*;

use std::net;
use url::Url;

#[derive(Parser, Clone)]
pub struct Cli {
    /// Listen on this address
    #[arg(long, default_value = "[::]:443")]
    pub bind: net::SocketAddr,

    /// The TLS configuration.
    #[command(flatten)]
    pub tls: moq_native_ietf::tls::Args,

    /// Forward all announces to the provided server for authentication/routing.
    /// If not provided, the relay accepts every unique announce.
    #[arg(long)]
    pub announce: Option<Url>,

    /// The URL of the moq-api server in order to run a cluster.
    /// Must be used in conjunction with --node to advertise the origin
    #[arg(long)]
    pub api: Option<Url>,

    /// The hostname that we advertise to other origins.
    /// The provided certificate must be valid for this address.
    #[arg(long)]
    pub node: Option<Url>,

    /// Stop the relay if the number of streams opened for a given subscription
    /// exceeds this value. The metric is the same as the Stream Count defined
    /// by the draft.
    ///
    /// If nothing is given, the feature is disabled.
    #[arg(long)]
    pub stream_limit: Option<u64>,

    /// Shutdown the relay before a subscription starts serving groups with a
    /// Group ID greater than or equal to this value.

    /// Stop the relay when the group to be served for a given subscription has
    /// a higher Group ID than this value.
    ///
    /// If nothing is given, the feature is disabled.
    #[arg(long)]
    pub max_group_id: Option<u64>,

    /// Enable development mode.
    /// This hosts a HTTPS web server via TCP to serve the fingerprint of the certificate.
    #[arg(long)]
    pub dev: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::builder()
        .format_timestamp_millis()
        .init();

    // Disable tracing so we don't get a bunch of Quinn spam.
    let tracer = tracing_subscriber::FmtSubscriber::builder()
        .with_max_level(tracing::Level::WARN)
        .finish();
    tracing::subscriber::set_global_default(tracer).unwrap();

    let cli = Cli::parse();
    let tls = cli.tls.load()?;

    if tls.server.is_none() {
        anyhow::bail!("missing TLS certificates");
    }

    // Create a QUIC server for media.
    let relay = Relay::new(RelayConfig {
        tls: tls.clone(),
        bind: cli.bind,
        node: cli.node,
        api: cli.api,
        announce: cli.announce,
        stream_limit: cli.stream_limit,
        max_group_id: cli.max_group_id,
    })?;

    if cli.dev {
        // Create a web server too.
        // Currently this only contains the certificate fingerprint (for development only).
        let web = Web::new(WebConfig {
            bind: cli.bind,
            tls,
        });

        tokio::spawn(async move {
            web.run().await.expect("failed to run web server");
        });
    }

    relay.run().await
}
