use core::time;
use std::{
    net::{self, SocketAddr},
    sync::Arc,
};

use anyhow::{Context, Error};
use clap::Parser;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use tokio::sync::Mutex;
use url::Url;

use moq_native_ietf::quic;
use moq_sub::{
    media::InitMode,
    multipath::{FailoverMethod, MultipathOptions, MultipathOut, SubscriberParams},
};
use moq_sub::{media::Media, multipath::SkipMode};
use moq_transport::{
    coding::Tuple,
    serve::Tracks,
    session::{Session, Subscriber},
};

const DEFAULT_BANDWIDTH_HINT: u64 = 2_000_000;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::builder().format_timestamp_millis().init();

    // Disable tracing so we don't get a bunch of Quinn spam.
    let tracer = tracing_subscriber::FmtSubscriber::builder()
        .with_max_level(tracing::Level::WARN)
        .finish();
    tracing::subscriber::set_global_default(tracer).unwrap();

    let config = Config::parse();

    let namespace = Tuple::from_utf8_path(&config.name);

    let failover_method = if config.adaptive_skip {
        FailoverMethod::DynamicAdaptive
    } else {
        let skip_mode = if config.skip_ahead == 0 {
            SkipMode::Disabled
        } else {
            let n = config.skip_ahead;
            match config.skip_unit {
                SkipUnit::Object => SkipMode::SameGroupNextObject(n),
                SkipUnit::Group => SkipMode::NextGroupFirstObject(n),
            }
        };
        FailoverMethod::Static(skip_mode)
    };

    let out = Arc::new(Mutex::new(MultipathOut::new(
        tokio::io::stdout(),
        MultipathOptions {
            non_preemptive_filtering: config.non_preemptive_filtering,
        },
    )));

    let mut tasks = FuturesUnordered::new();

    'url_probing: for (i, url) in config.urls.iter().enumerate() {
        // Create a QUIC session and a corresponding media runner
        let (session, subscriber, tracks) =
            create_session(&config.tls, config.bind, url, namespace.clone(), config.idle_duration_ms).await?;
        log::debug!("session {} started for url {:?}.", i, url);

        let session_id = (i + 1) as u64; // workaround as session.session_id() always gives 0 for some reason
        let connection = session.connection();

        let subscriber_params = SubscriberParams {
            failover_method: failover_method.clone(),
            bandwidth_hint: *config.bandwidth_hints.get(i).unwrap_or(&DEFAULT_BANDWIDTH_HINT),
        };

        let mut media = Media::new(session_id, connection, subscriber, tracks, out.clone()).await?;

        tasks.push(tokio::spawn(async move {
            session.run().await.or_else(|e| Err(format!("{:?}", e)))
        }));
        tasks.push(tokio::spawn(async move {
            media.run(
                if i == 0 { InitMode::InitTrack } else { InitMode::Direct("1.m4s".to_string()) },
                subscriber_params,
            ).await.or_else(|e| Err(format!("{:?}", e)))
        }));

        // Wait for the media to finish or an error (e.g., connerr) to occur
        if !config.multipath {
            while let Some(finished_task) = tasks.next().await {
                match finished_task {
                    Err(e) => {
                        log::error!("round#{}, {:?}", i, e);
                    }
                    Ok(result) => {
                        log::debug!("round#{}, task result: {:?}", i, result);
                        if let Err(msg) = result {
                            if msg == "Finished receiving the media" {
                                break 'url_probing;
                            }
                        }
                        break;
                    }
                }
            }

            tasks.clear();

            // Note: currently the multipath-enhanced output object doesn't make much sense for use in the reconnect
            // strategy, but it's needed for compatibility with the media runner. Also, once it is merged with the
            // multipath branch, it will be crucial to achieve a converged solution where multipath and reconnect
            // co-exist to make MoQ resilient.
            out.lock().await.clear_subscribers();
        }
    }

    if config.multipath {
        while let Some(finished_task) = tasks.next().await {
            match finished_task {
                Err(e) => {
                    log::error!("multipath, {:?}", e);
                }
                Ok(result) => {
                    log::debug!("multipath, task result: {:?}", result);
                    if let Err(msg) = result {
                        if msg == "Finished receiving the media" {
                            break;
                        }
                    }
                }
            }
        }
    }

    Ok(())
}

async fn create_session(
    tls: &moq_native_ietf::tls::Args,
    bind: SocketAddr,
    url: &Url,
    namespace: Tuple,
    idle_duration_ms: u64,
) -> Result<(Session, Subscriber, Tracks), Error> {
    let tls = tls.load()?;
    let quic = quic::Endpoint::new(quic::Config {
        bind,
        tls,
        idle_duration: time::Duration::from_millis(idle_duration_ms),
    })?;

    let session = quic.client.connect(url).await?;

    let (session, subscriber) = moq_transport::session::Subscriber::connect(session)
        .await
        .context("failed to create MoQ Transport session")?;

    // Associate empty set of Tracks with provided namespace
    let tracks = Tracks::new(namespace);

    Ok((session, subscriber, tracks))
}

#[derive(clap::ValueEnum, Clone, Default, Debug)]
pub enum SkipUnit {
    /// Skip ahead SKIP_AHEAD objects within the current group.
    Object,
    /// Skip ahead SKIP_AHEAD number of groups.
    #[default]
    Group,
}

#[derive(Parser, Clone)]
pub struct Config {
    /// Listen for UDP packets on the given address.
    #[arg(long, default_value = "[::]:0")]
    pub bind: net::SocketAddr,

    /// The TLS configuration.
    #[command(flatten)]
    pub tls: moq_native_ietf::tls::Args,

    /// Time to wait before concluding the idle connection as closed.
    #[arg(long, default_value = "1000")]
    pub idle_duration_ms: u64,

    /// Establish WebTransport sessions to the given URLs starting with https://
    #[arg(value_parser = moq_url)]
    pub urls: Vec<Url>,

    /// Whether to establish and use multiple connections simultaneously to
    /// receive media.
    #[arg(long)]
    pub multipath: bool,

    /// Try not to download this many units of data on other paths
    ///
    /// When moq-sub receives an object from a relay, it notifies
    /// alternative relays not to send this object and other objects
    /// about to be recieved from this relay.  See --skip-unit.
    /// Default: disabled.
    #[arg(long, default_value_t = 0)]
    pub skip_ahead: u64,

    /// The unit of --skip-ahead
    #[clap(long, default_value_t, value_enum)]
    pub skip_unit: SkipUnit,

    /// Set the skip-ahead offset dynamically based on the environment (i.e., use the adapative method)
    /// When provided, the --skip-unit and --skip-ahead arguments are ignored.
    #[arg(long)]
    pub adaptive_skip: bool,

    /// Hint used by the dynamic adaptive method for estimating the bandwidth on each path.
    /// Default: 2 Mbps on every path.
    #[arg(long)]
    pub bandwidth_hints: Vec<u64>,

    /// Request non-preemptive handling of subscription filters from relay(s).
    /// This behavior of the relay does not conform to the draft, therefore,
    /// this feature requires special relays to work.
    #[arg(long)]
    pub non_preemptive_filtering: bool,

    /// The name of the broadcast
    #[arg(long)]
    pub name: String,
}

fn moq_url(s: &str) -> Result<Url, String> {
    let url = Url::try_from(s).map_err(|e| e.to_string())?;

    // Make sure the scheme is moq
    if url.scheme() != "https" && url.scheme() != "moqt" {
        return Err("url scheme must be https:// for WebTransport & moqt:// for QUIC".to_string());
    }

    Ok(url)
}
