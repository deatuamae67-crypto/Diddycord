use crate::{
    FrontendChannel, FrontendChannelChange, FrontendEvent, FrontendGuildSnapshot,
    FrontendGuildUpdate,
};

pub const DEFAULT_MAX_GUILDS: usize = 256;
pub const DEFAULT_MAX_CHANNELS_PER_GUILD: usize = 512;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GuildRef<'a> {
    pub id: &'a str,
    pub name: &'a str,
    pub unavailable: bool,
    pub channel_count: usize,
}

struct CachedGuild {
    id: Box<str>,
    name: Box<str>,
    unavailable: bool,
    channels: Vec<FrontendChannel>,
    touched: u64,
}

pub struct TopologyState {
    guilds: Vec<CachedGuild>,
    max_guilds: usize,
    max_channels_per_guild: usize,
    clock: u64,
    dropped_items: u64,
}

impl Default for TopologyState {
    fn default() -> Self {
        Self::new(DEFAULT_MAX_GUILDS, DEFAULT_MAX_CHANNELS_PER_GUILD)
    }
}

impl TopologyState {
    pub fn new(max_guilds: usize, max_channels_per_guild: usize) -> Self {
        let max_guilds = max_guilds.max(1);
        let max_channels_per_guild = max_channels_per_guild.max(1);
        Self {
            guilds: Vec::with_capacity(max_guilds.min(32)),
            max_guilds,
            max_channels_per_guild,
            clock: 0,
            dropped_items: 0,
        }
    }

    pub fn guild_count(&self) -> usize {
        self.guilds.len()
    }

    pub fn dropped_items(&self) -> u64 {
        self.dropped_items
    }

    pub fn guild(&self, guild_id: &str) -> Option<GuildRef<'_>> {
        self.guilds
            .iter()
            .find(|guild| guild.id.as_ref() == guild_id)
            .map(guild_ref)
    }

    pub fn guilds(&self) -> impl Iterator<Item = GuildRef<'_>> {
        self.guilds.iter().map(guild_ref)
    }

    pub fn channels<'a>(
        &'a self,
        guild_id: &'a str,
    ) -> impl Iterator<Item = &'a FrontendChannel> + 'a {
        self.guilds
            .iter()
            .find(|guild| guild.id.as_ref() == guild_id)
            .into_iter()
            .flat_map(|guild| guild.channels.iter())
    }

    pub fn channel(&self, guild_id: &str, channel_id: &str) -> Option<&FrontendChannel> {
        self.guilds
            .iter()
            .find(|guild| guild.id.as_ref() == guild_id)
            .and_then(|guild| {
                guild
                    .channels
                    .iter()
                    .find(|channel| channel.id.as_ref() == channel_id)
            })
    }

    pub(crate) fn apply(&mut self, event: &FrontendEvent) -> bool {
        match event {
            FrontendEvent::GuildCreate(guild) => {
                self.apply_guild_snapshot(guild);
                true
            }
            FrontendEvent::GuildUpdate(update) => {
                self.apply_guild_update(update);
                true
            }
            FrontendEvent::GuildDelete(delete) => {
                if delete.unavailable {
                    self.mark_guild_unavailable(delete.id.as_ref());
                } else {
                    self.guilds
                        .retain(|guild| guild.id.as_ref() != delete.id.as_ref());
                }
                true
            }
            FrontendEvent::ChannelCreate(change) | FrontendEvent::ChannelUpdate(change) => {
                self.apply_channel_change(change);
                true
            }
            FrontendEvent::ChannelDelete(delete) => {
                if let Some(guild) = self
                    .guilds
                    .iter_mut()
                    .find(|guild| guild.id.as_ref() == delete.guild_id.as_ref())
                {
                    guild
                        .channels
                        .retain(|channel| channel.id.as_ref() != delete.channel_id.as_ref());
                    self.clock = self.clock.saturating_add(1);
                    guild.touched = self.clock;
                }
                true
            }
            _ => false,
        }
    }

    fn apply_guild_snapshot(&mut self, snapshot: &FrontendGuildSnapshot) {
        self.clock = self.clock.saturating_add(1);
        let touched = self.clock;

        if let Some(guild) = self
            .guilds
            .iter_mut()
            .find(|guild| guild.id.as_ref() == snapshot.id.as_ref())
        {
            guild.name = snapshot.name.clone();
            guild.unavailable = snapshot.unavailable;
            guild.touched = touched;
            if !(snapshot.unavailable && snapshot.channels.is_empty()) {
                let dropped = snapshot
                    .channels
                    .len()
                    .saturating_sub(self.max_channels_per_guild);
                self.dropped_items = self.dropped_items.saturating_add(dropped as u64);
                guild.channels = snapshot
                    .channels
                    .iter()
                    .take(self.max_channels_per_guild)
                    .cloned()
                    .collect();
            }
            return;
        }

        if self.guilds.len() >= self.max_guilds {
            self.evict_least_recently_used_guild();
        }

        let dropped = snapshot
            .channels
            .len()
            .saturating_sub(self.max_channels_per_guild);
        self.dropped_items = self.dropped_items.saturating_add(dropped as u64);

        self.guilds.push(CachedGuild {
            id: snapshot.id.clone(),
            name: snapshot.name.clone(),
            unavailable: snapshot.unavailable,
            channels: snapshot
                .channels
                .iter()
                .take(self.max_channels_per_guild)
                .cloned()
                .collect(),
            touched,
        });
    }

    fn apply_guild_update(&mut self, update: &FrontendGuildUpdate) {
        let Some(guild) = self
            .guilds
            .iter_mut()
            .find(|guild| guild.id.as_ref() == update.id.as_ref())
        else {
            return;
        };

        if let Some(name) = update.name.as_ref() {
            guild.name = name.clone();
        }
        if let Some(unavailable) = update.unavailable {
            guild.unavailable = unavailable;
        }
        self.clock = self.clock.saturating_add(1);
        guild.touched = self.clock;
    }

    fn mark_guild_unavailable(&mut self, guild_id: &str) {
        let Some(guild) = self
            .guilds
            .iter_mut()
            .find(|guild| guild.id.as_ref() == guild_id)
        else {
            return;
        };
        self.clock = self.clock.saturating_add(1);
        guild.touched = self.clock;
        guild.unavailable = true;
    }

    fn apply_channel_change(&mut self, change: &FrontendChannelChange) {
        let Some(guild) = self
            .guilds
            .iter_mut()
            .find(|guild| guild.id.as_ref() == change.guild_id.as_ref())
        else {
            return;
        };

        self.clock = self.clock.saturating_add(1);
        guild.touched = self.clock;

        if let Some(channel) = guild
            .channels
            .iter_mut()
            .find(|channel| channel.id.as_ref() == change.channel.id.as_ref())
        {
            *channel = change.channel.clone();
            return;
        }

        if guild.channels.len() >= self.max_channels_per_guild {
            self.dropped_items = self.dropped_items.saturating_add(1);
            return;
        }

        guild.channels.push(change.channel.clone());
    }

    fn evict_least_recently_used_guild(&mut self) {
        let Some((index, guild)) = self
            .guilds
            .iter()
            .enumerate()
            .min_by_key(|(_, guild)| guild.touched)
        else {
            return;
        };

        let dropped = 1u64.saturating_add(guild.channels.len() as u64);
        self.dropped_items = self.dropped_items.saturating_add(dropped);
        self.guilds.remove(index);
    }
}

fn guild_ref(guild: &CachedGuild) -> GuildRef<'_> {
    GuildRef {
        id: guild.id.as_ref(),
        name: guild.name.as_ref(),
        unavailable: guild.unavailable,
        channel_count: guild.channels.len(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        FrontendChannelDelete, FrontendGuildDelete, FrontendGuildSnapshot, FrontendGuildUpdate,
    };

    fn channel(id: &str, name: &str, position: i32) -> FrontendChannel {
        FrontendChannel {
            id: Box::<str>::from(id),
            name: Some(Box::<str>::from(name)),
            kind: 0,
            position,
            parent_id: None,
        }
    }

    fn guild(id: &str, name: &str, channels: Vec<FrontendChannel>) -> FrontendEvent {
        FrontendEvent::GuildCreate(FrontendGuildSnapshot {
            id: Box::<str>::from(id),
            name: Box::<str>::from(name),
            unavailable: false,
            channels,
        })
    }

    #[test]
    fn guild_snapshot_keeps_only_bounded_topology() {
        let mut state = TopologyState::new(2, 2);
        state.apply(&guild(
            "1",
            "one",
            vec![channel("10", "a", 0), channel("11", "b", 1), channel("12", "c", 2)],
        ));

        assert_eq!(state.guild_count(), 1);
        assert_eq!(state.guild("1").unwrap().channel_count, 2);
        assert_eq!(state.dropped_items(), 1);
        assert!(state.channel("1", "12").is_none());
    }

    #[test]
    fn channel_create_update_delete_are_applied_in_place() {
        let mut state = TopologyState::new(4, 8);
        state.apply(&guild("1", "one", vec![channel("10", "old", 0)]));

        state.apply(&FrontendEvent::ChannelUpdate(FrontendChannelChange {
            guild_id: Box::<str>::from("1"),
            channel: channel("10", "new", 4),
        }));
        assert_eq!(state.channel("1", "10").unwrap().name.as_deref(), Some("new"));
        assert_eq!(state.channel("1", "10").unwrap().position, 4);

        state.apply(&FrontendEvent::ChannelCreate(FrontendChannelChange {
            guild_id: Box::<str>::from("1"),
            channel: channel("11", "second", 5),
        }));
        assert_eq!(state.guild("1").unwrap().channel_count, 2);

        state.apply(&FrontendEvent::ChannelDelete(FrontendChannelDelete {
            guild_id: Box::<str>::from("1"),
            channel_id: Box::<str>::from("10"),
        }));
        assert!(state.channel("1", "10").is_none());
        assert_eq!(state.guild("1").unwrap().channel_count, 1);
    }

    #[test]
    fn unavailable_delete_preserves_topology_but_true_delete_removes_it() {
        let mut state = TopologyState::new(4, 8);
        state.apply(&guild("1", "one", vec![channel("10", "a", 0)]));
        state.apply(&FrontendEvent::GuildDelete(FrontendGuildDelete {
            id: Box::<str>::from("1"),
            unavailable: true,
        }));

        assert!(state.guild("1").unwrap().unavailable);
        assert_eq!(state.guild("1").unwrap().channel_count, 1);

        state.apply(&FrontendEvent::GuildDelete(FrontendGuildDelete {
            id: Box::<str>::from("1"),
            unavailable: false,
        }));
        assert!(state.guild("1").is_none());
    }

    #[test]
    fn guild_update_changes_name_without_rebuilding_channels() {
        let mut state = TopologyState::new(4, 8);
        state.apply(&guild("1", "old", vec![channel("10", "a", 0)]));
        state.apply(&FrontendEvent::GuildUpdate(FrontendGuildUpdate {
            id: Box::<str>::from("1"),
            name: Some(Box::<str>::from("new")),
            unavailable: None,
        }));

        let guild = state.guild("1").unwrap();
        assert_eq!(guild.name, "new");
        assert_eq!(guild.channel_count, 1);
    }

    #[test]
    fn least_recently_used_guild_is_evicted_at_capacity() {
        let mut state = TopologyState::new(2, 4);
        state.apply(&guild("1", "one", vec![channel("10", "a", 0)]));
        state.apply(&guild("2", "two", vec![]));
        state.apply(&FrontendEvent::GuildUpdate(FrontendGuildUpdate {
            id: Box::<str>::from("1"),
            name: None,
            unavailable: Some(false),
        }));
        state.apply(&guild("3", "three", vec![]));

        assert!(state.guild("1").is_some());
        assert!(state.guild("2").is_none());
        assert!(state.guild("3").is_some());
        assert!(state.dropped_items() >= 1);
    }
}
