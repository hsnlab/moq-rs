use std::{
    net::{self, SocketAddr},
    sync::Arc,
};

use anyhow::{Context, Error};
use clap::Parser;
use tokio::sync::Mutex;
use url::Url;

use moq_native_ietf::quic;
use moq_sub::media::Media;
use moq_sub::smartout::SmartOut;
use moq_transport::{
    coding::Tuple,
    serve::Tracks,
    session::{Session, Subscriber},
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();

    // Disable tracing so we don't get a bunch of Quinn spam.
    let tracer = tracing_subscriber::FmtSubscriber::builder()
        .with_max_level(tracing::Level::WARN)
        .finish();
    tracing::subscriber::set_global_default(tracer).unwrap();

    let out = Arc::new(Mutex::new(SmartOut::new(tokio::io::stdout())));

    let config = Config::parse();

    let namespace = Tuple::from_utf8_path(&config.name);
    let (session1, subscriber1, tracks1) = create_session(
        &config.tls,
        config.bind,
        &config.url_primary,
        namespace.clone(),
    )
    .await?;
    log::debug!("😶: session1 started.");
    let (session2, subscriber2, tracks2) = create_session(
        &config.tls,
        config.bind,
        &config.url_secondary,
        namespace.clone(),
    )
    .await?;
    log::debug!("😶: session2 started.");

    let mut media1 = Media::new(subscriber1, tracks1, out.clone(), "Relay 1".to_string()).await?;
    let mut media2 = Media::new(subscriber2, tracks2, out.clone(), "Relay 2".to_string()).await?;
    tokio::select! {
        res = session1.run() => res.context("session error (1)")?,
        res = session2.run() => res.context("session error (2)")?,
        res = media1.run() => res.context("media error")?,
        res = media2.run() => res.context("media error")?,
    }

    Ok(())
}

async fn create_session(
    tls: &moq_native_ietf::tls::Args,
    bind: SocketAddr,
    url: &Url,
    namespace: Tuple,
) -> Result<(Session, Subscriber, Tracks), Error> {
    let tls = tls.load()?;
    let quic = quic::Endpoint::new(quic::Config { bind, tls })?;

    let session = quic.client.connect(url).await?;

    let (session, subscriber) = moq_transport::session::Subscriber::connect(session)
        .await
        .context("failed to create MoQ Transport session")?;

    // Associate empty set of Tracks with provided namespace
    let tracks = Tracks::new(namespace);

    Ok((session, subscriber, tracks))
}

#[derive(Parser, Clone)]
pub struct Config {
    /// Listen for UDP packets on the given address.
    #[arg(long, default_value = "[::]:0")]
    pub bind: net::SocketAddr,

    /// Connect to the given URL starting with https://
    #[arg(value_parser = moq_url)]
    pub url_primary: Url,

    /// Connect to the given URL starting with https://
    #[arg(value_parser = moq_url)]
    pub url_secondary: Url,

    /// The name of the broadcast
    #[arg(long)]
    pub name: String,

    /// The TLS configuration.
    #[command(flatten)]
    pub tls: moq_native_ietf::tls::Args,
}

fn moq_url(s: &str) -> Result<Url, String> {
    let url = Url::try_from(s).map_err(|e| e.to_string())?;

    // Make sure the scheme is moq
    if url.scheme() != "https" && url.scheme() != "moqt" {
        return Err("url scheme must be https:// for WebTransport & moqt:// for QUIC".to_string());
    }

    Ok(url)
}
