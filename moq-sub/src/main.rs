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
use moq_sub::{media::InitMode, multipath::MultipathOut};
use moq_sub::{media::Media, multipath::SkipMode};
use moq_transport::{
    coding::Tuple,
    serve::Tracks,
    session::{Session, Subscriber},
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::builder().format_timestamp_millis().init();

    // Disable tracing so we don't get a bunch of Quinn spam.
    let tracer = tracing_subscriber::FmtSubscriber::builder()
        .with_max_level(tracing::Level::WARN)
        .finish();
    tracing::subscriber::set_global_default(tracer).unwrap();

    let config = Config::parse();

    let out = Arc::new(Mutex::new(MultipathOut::new(
        tokio::io::stdout(),
        if config.skip_ahead == 0 {
            SkipMode::Disabled
        } else {
            let n = config.skip_ahead;
            match config.skip_unit {
                SkipUnit::Object => SkipMode::SameGroupNextObject(n),
                SkipUnit::Group => SkipMode::NextGroupFirstObject(n),
            }
        },
    )));

    let namespace = Tuple::from_utf8_path(&config.name);
    let mut tasks = FuturesUnordered::new();

    'url_probing: for (i, url) in config.urls.iter().enumerate() {
        let (session, subscriber, tracks) =
            create_session(&config.tls, config.bind, url, namespace.clone(), config.idle_duration_ms).await?;
        log::debug!("session {} started for url {:?}.", i, url);
        let session_id = (i + 1) as u64; // workaround, since session.webtransport.0.session_id is private on multiple levels
        let mut media = Media::new(session_id, subscriber, tracks, out.clone()).await?;
        tasks.push(tokio::spawn(async move {
            session.run().await.or_else(|e| Err(format!("{:?}", e)))
        }));
        tasks.push(tokio::spawn(async move {
            media.run(
                if i == 0 { InitMode::InitTrack } else { InitMode::Direct("1.m4s".to_string()) }
            ).await.or_else(|e| Err(format!("{:?}", e)))
        }));
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
                    tasks.clear();
                    break;
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
    let quic = quic::Endpoint::new(quic::Config { bind, tls, idle_duration: time::Duration::from_millis(idle_duration_ms) })?;

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
