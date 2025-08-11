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

    /// The SubscribePair compromising the Group ID and the Object ID specified in the most
    /// recent subscription update, and the Session ID of which session it was sent from.
    last_update: Option<(SubscribePair, u64)>,

    /// The number of distinct objects received in the context of this track.
    n_unique: u64,
    /// The number of duplicate objects received in the context of this track.
    n_duplicate: u64,

    // Given
    // - "A subscriber MUST NOT make multiple active subscriptions for a track within a single session [...]"
    // - "Subscribe ID is a variable length integer that MUST be unique [...]"
    // we can conclude, that Subscribe IDs present here will correspond to one and only one Track (in the context of a
    // session).
    // However, to identify which session sent a certain object and consequently which sessions to update, we need to
    // store the artificial session IDs alongside, that the caller of write_group_object has to specify.
    subscribers: Vec<(u64, u64, Subscriber)>,
}

#[derive(Clone, Debug)]
pub enum SkipMode {
    Disabled,
    SameGroupNextObject(u64),
    NextGroupFirstObject(u64),
}

#[derive(Clone, Debug)]
pub struct MultipathOptions {
    pub skip_mode: SkipMode,
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
            .expect("trying to write object with no corresponding subscriptions");

        if let Some(last_id) = playout.last_id {
            if id <= last_id {
                // Already seen this pair
                playout.n_duplicate += 1;
                log::trace!(
                    "object action: drop, session_id: {}, group_id: {}, subgroup_id: {}, object_id: {}",
                    sender_session_id,
                    object.group_id,
                    object.subgroup_id,
                    object.object_id
                );
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
        let (next, next_filter) = match self.options.skip_mode {
            SkipMode::Disabled => return Ok(()),
            SkipMode::SameGroupNextObject(n) => {
                let next = SubscribePair {
                    group: group_id,
                    object: object_id + n,
                };
                (next.clone(), SubscribeFilter::AbsoluteStart(next))
            }
            SkipMode::NextGroupFirstObject(n) => {
                let next = SubscribePair {
                    group: group_id + n,
                    object: 0,
                };
                (next.clone(), SubscribeFilter::AbsoluteStart(next))
            }
        };

        // Ask the relays from other sessions to skip ahead and not to send
        // objects we have already recevied or about to receive shortly.

        if let Some((update_target, updater_session_id)) = &playout.last_update {
            // Either the playout surpassed the prior update target, or we
            // received the current object from the same relay as before, and
            // we can send out the update while adhering to the options.
            if current >= *update_target
                || (*updater_session_id == sender_session_id
                    && !(self.options.consolidated_updates && next == *update_target))
            {
                Self::subscribe_update(
                    playout,
                    sender_session_id,
                    next,
                    next_filter,
                    self.options.non_preemptive_filtering,
                );
            }
        } else {
            Self::subscribe_update(
                playout,
                sender_session_id,
                next,
                next_filter,
                self.options.non_preemptive_filtering,
            );
        }

        Ok(())
    }

    fn subscribe_update(
        playout: &mut TrackPlayoutStatus,
        sender_session_id: u64,
        next: SubscribePair,
        next_filter: SubscribeFilter,
        non_preemptive_filtering: bool,
    ) {
        for (session_id, subscribe_id, subscriber) in &mut playout.subscribers {
            if *session_id == sender_session_id {
                continue;
            }

            let _ = subscriber.subscribe_update(
                *subscribe_id,
                next_filter.clone(),
                non_preemptive_filtering,
                127,
            );
        }
        playout.last_update = Some((next, sender_session_id));
    }

    pub fn add_subscriber(
        self: &mut Self,
        key: String,
        session_id: u64,
        subscribe_id: u64,
        subscriber: Subscriber,
    ) {
        if let Some(playout) = self.tracks.get_mut(&key) {
            playout
                .subscribers
                .push((session_id, subscribe_id, subscriber));
        } else {
            self.tracks.insert(
                key,
                TrackPlayoutStatus {
                    last_id: None,
                    last_update: None,
                    n_unique: 0,
                    n_duplicate: 0,
                    subscribers: vec![(session_id, subscribe_id, subscriber)],
                },
            );
        }
    }

    pub fn remove_subscriber(self: &mut Self, key: String, session_id: u64) {
        if let Some(playout) = self.tracks.get_mut(&key) {
            if let Some(index) = playout
                .subscribers
                .iter()
                .position(|(subscriber_session_id, _, _)| *subscriber_session_id == session_id)
            {
                playout.subscribers.remove(index);
            }
        }
    }

    pub fn clear_subscribers(self: &mut Self) {
        for playout in self.tracks.values_mut() {
            playout.subscribers.clear();
        }
    }
}
