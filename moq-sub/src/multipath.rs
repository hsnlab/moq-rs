use moq_transport::serve::SubgroupObjectReader;
use moq_transport::{
    message::SubscribePair,
    serve::Track,
    session::{SubscribeFilter, Subscriber},
};
use std::collections::HashMap;
use tokio::io::{AsyncWrite, AsyncWriteExt};

struct TrackPlayoutStatus {
    /// The Group ID and Object ID of the most recently received object if there were any.
    last_id: Option<(u64, u64)>,

    /// The total number of distinct objects received.
    n_unique: u64,

    subscriptions: HashMap<u64, Subscription>,
}

#[derive(Debug, Clone)]
pub struct SubscriberParams {
    pub failover_method: FailoverMethod,
    pub bandwidth_hint: u64,
}


pub struct Subscription {
    // Given
    // - "A subscriber MUST NOT make multiple active subscriptions for a track within a single session [...]"
    // - "Subscribe ID is a variable length integer that MUST be unique [...]"
    // we can conclude, that Subscribe IDs present here will correspond to one and only one Track (in the context of a
    // session).
    // However, to identify which session sent a certain object and consequently which sessions to update, we need to
    // store the artificial session IDs alongside, that the caller of write_group_object has to specify.

    /// Artificial session ID created by the moq-sub. Note, this is not the same as the session ID used by QUIC/WebTransport.
    session_id: u64,
    /// Subscribe ID used between the subscriber and a relay
    subscribe_id: u64,
    /// The underlying QUIC connection of this subscription.
    connection: moq_native_ietf::quic::Connection,
    /// Subscriber providing the subscription.
    subscriber: Subscriber,
    /// Failover method to be used for this subscription.
    failover_method: FailoverMethod,
    /// Bandwidth hint
    bandwidth_hint: u64,

    /// The SubscribePair compromising the Group ID and the Object ID specified in the most
    /// recent subscription update, and the Session ID of which session it was sent from.
    last_update: Option<(SubscribePair, u64)>,
    /// The number of duplicate objects received from this subscription.
    n_duplicate: u64,
}

#[derive(Clone, Debug)]
pub enum SkipMode {
    Disabled,
    SameGroupNextObject(u64),
    NextGroupFirstObject(u64),
}

impl SkipMode {
    fn skip_target(&self, current: SubscribePair) -> Option<SubscribePair> {
        match self {
            SkipMode::Disabled => return None,
            SkipMode::SameGroupNextObject(n) => {
                Some(SubscribePair {
                    group: current.group,
                    object: current.object + n,
                })
            }
            SkipMode::NextGroupFirstObject(n) => {
                Some(SubscribePair {
                    group: current.group + n,
                    object: 0,
                })
            }
        }
    }
}

#[derive(Clone, Debug)]
pub enum FailoverMethod {
    /// Static mode uses a fixed skip mode throughout the track given as a parameter.
    Static(SkipMode),
    /// Dynamically adapts to the environment observed through QUIC statistics.
    DynamicAdaptive,
}

#[derive(Clone, Debug)]
pub struct MultipathOptions {
    pub non_preemptive_filtering: bool,
    pub consolidated_updates: bool,
}

pub struct MultipathOut<O: AsyncWrite + Send + Unpin + 'static> {
    out: O,
    tracks: HashMap<String, TrackPlayoutStatus>, // For each unique track
    options: MultipathOptions,
}

impl<O: AsyncWrite + Send + Unpin + 'static> MultipathOut<O> {
    pub fn new(out: O, options: MultipathOptions) -> Self {
        Self {
            out,
            tracks: HashMap::new(),
            options,
        }
    }

    pub fn create_key(track: &Track) -> String {
        track.namespace.to_utf8_path() + ":" + &track.name
    }

    pub async fn write_object(
        &mut self,
        sender_session_id: u64,
        object: SubgroupObjectReader,
        buf: &Vec<u8>,
    ) -> Result<(), std::io::Error> {
        let key = Self::create_key(&object.group);
        return self
            .write_group_object(sender_session_id, key, object, buf)
            .await;
    }

    pub async fn write_group_object(
        &mut self,
        sender_session_id: u64,
        key: String,
        object: SubgroupObjectReader,
        buf: &Vec<u8>,
    ) -> Result<(), std::io::Error> {
        let group_id = object.group_id;
        let object_id = object.object_id;

        let id = (group_id, object_id);

        let playout = self
            .tracks
            .get_mut(&key)
            .expect("write object of a track with active subscriptions");

        let subscription = playout
            .subscriptions
            .get_mut(&sender_session_id)
            .expect("sender session ID corresponds to an existent subscription");

        if let Some(last_id) = playout.last_id {
            if id <= last_id {
                // Already seen this pair
                log::trace!(
                    "object action: drop, session_id: {}, group_id: {}, subgroup_id: {}, object_id: {}",
                    sender_session_id,
                    object.group_id,
                    object.subgroup_id,
                    object.object_id
                );
                subscription.n_duplicate += 1;
                return Ok(());
            }
        }
        playout.last_id = Some(id);
        playout.n_unique += 1;

        log::trace!(
            "object action: playout, session_id: {}, group_id: {}, subgroup_id: {}, object_id: {}",
            sender_session_id,
            object.group_id,
            object.subgroup_id,
            object.object_id
        );
        self.out.write_all(&buf).await?;

        let current = SubscribePair {
            group: group_id,
            object: object_id,
        };

        // Ask the relays from other sessions to skip ahead and not to send
        // objects we have already recevied or about to receive shortly.
        Self::skip_ahead(
            playout,
            sender_session_id,
            current,
            self.options.non_preemptive_filtering,
            self.options.consolidated_updates,
        );

        Ok(())
    }

    fn skip_ahead(
        playout: &mut TrackPlayoutStatus,
        sender_session_id: u64,
        current: SubscribePair,
        non_preemptive_filtering: bool,
        consolidated_updates: bool,
    ) {
        let (sender_stats, sender_bw) = {
            let subscription = playout.subscriptions.get(&sender_session_id).expect("object sender exists");
            let connection = &subscription.connection;
            let stats = connection.stats().path.clone();
            let bw = subscription.bandwidth_hint;
            (stats, bw)
        };
        for subscription in &mut playout.subscriptions.values_mut() {
            if subscription.session_id == sender_session_id {
                continue;
            }

            let skip_mode = match &subscription.failover_method {
                FailoverMethod::Static(skip_mode) => skip_mode,
                FailoverMethod::DynamicAdaptive => {
                    let receiver_stats = subscription.connection.stats().path;

                    let rtt_r1_sub = sender_stats.rtt.as_nanos() as f64 / 1e9;
                    let d_r1_sub = rtt_r1_sub / 2.0;

                    let rtt_r2_sub = receiver_stats.rtt.as_nanos() as f64 / 1e9;
                    let d_r2_sub = rtt_r2_sub / 2.0;

                    let s = 50_000.0; // TODO: remove hard-coded object size
                    let r = sender_bw as f64;

                    let t_s = d_r1_sub + s / r + d_r2_sub;

                    let t_o = 1.0 / 15.0; // TODO: remove hard-coded FPS

                    let n_d = t_s / t_o;
                    let n_s = n_d.ceil();

                    let m = 10 as f64; // TODO: remove hard-code object-per-group count

                    log::trace!("t_s = {} + {} / {} + {} = {0} + {} + {2} = {}", d_r1_sub, s, r, d_r2_sub, s / r, t_s);
                    log::trace!("n_s = {}", n_s);

                    if n_s == 1.0 && m > 1.0 {
                        log::debug!("using object=1");
                        &SkipMode::SameGroupNextObject(1)
                    } else {
                        let k = (n_s as f64 / m).ceil() as u64;
                        log::debug!("using group={}", k);
                        &SkipMode::NextGroupFirstObject(k)
                    }
                }
            };

            let Some(next) = skip_mode.skip_target(current.clone()) else { return; };
            let next_filter = SubscribeFilter::AbsoluteStart(next.clone());

            if let Some((last_target, last_session_id)) = &subscription.last_update {
                // The start location in the filter must not decrease.
                if next < *last_target {
                    continue;
                }

                // Only that session can update this subscription who did it the last time,
                // except when the playout surpassed the prior update target.
                if *last_session_id != sender_session_id && current < *last_target {
                    continue;
                }
            }

            let _ = subscription.subscriber.subscribe_update(
                subscription.subscribe_id,
                next_filter,
                non_preemptive_filtering,
                127,
            );
            subscription.last_update = Some((next, sender_session_id));
        }
    }

    pub fn add_subscriber(
        self: &mut Self,
        key: String,
        session_id: u64,
        subscribe_id: u64,
        connection: moq_native_ietf::quic::Connection,
        subscriber: Subscriber,
        subscriber_params: SubscriberParams,
    ) {
        let subscription = Subscription {
            session_id,
            subscribe_id,
            connection,
            subscriber,
            failover_method: subscriber_params.failover_method,
            bandwidth_hint: subscriber_params.bandwidth_hint,
            last_update: None,
            n_duplicate: 0,
        };
        if let Some(playout) = self.tracks.get_mut(&key) {
            playout.subscriptions.insert(session_id, subscription);
        } else {
            let mut subscriptions = HashMap::new();
            subscriptions.insert(session_id, subscription);
            self.tracks.insert(
                key,
                TrackPlayoutStatus {
                    last_id: None,
                    n_unique: 0,
                    subscriptions: subscriptions,
                },
            );
        }
    }

    pub fn remove_subscriber(self: &mut Self, key: String, session_id: u64) {
        if let Some(playout) = self.tracks.get_mut(&key) {
            let _ = playout.subscriptions.remove(&session_id);
        }
    }

    pub fn clear_subscribers(self: &mut Self) {
        for playout in self.tracks.values_mut() {
            playout.subscriptions.clear();
        }
    }
}
