use crate::{FrontendState, RestEvent, RestHandle, RestOperation, RestSubmitError};

const MIN_PAGE_SIZE: u8 = 1;
const MAX_PAGE_SIZE: u8 = 100;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HistoryLoadStatus {
    Submitted(u32),
    Busy,
    Exhausted,
    NoSelection,
    NoAnchor,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HistoryCompletion {
    Ignored,
    Loaded { count: usize, exhausted: bool },
    Failed { retryable: bool },
}

#[derive(Debug)]
struct PendingHistory {
    request_id: u32,
    channel_id: Box<str>,
    requested: u8,
    before: Option<Box<str>>,
}

pub struct HistoryPager {
    selected_channel: Option<Box<str>>,
    page_size: u8,
    pending: Option<PendingHistory>,
    exhausted: bool,
}

impl HistoryPager {
    pub fn new(page_size: u8) -> Self {
        Self {
            selected_channel: None,
            page_size: page_size.clamp(MIN_PAGE_SIZE, MAX_PAGE_SIZE),
            pending: None,
            exhausted: false,
        }
    }

    pub fn selected_channel(&self) -> Option<&str> {
        self.selected_channel.as_deref()
    }

    pub fn page_size(&self) -> u8 {
        self.page_size
    }

    pub fn pending_request_id(&self) -> Option<u32> {
        self.pending.as_ref().map(|pending| pending.request_id)
    }

    pub fn is_exhausted(&self) -> bool {
        self.exhausted
    }

    pub fn select_channel(&mut self, channel_id: &str) -> bool {
        if self.selected_channel.as_deref() == Some(channel_id) {
            return false;
        }

        self.selected_channel = Some(Box::<str>::from(channel_id));
        self.pending = None;
        self.exhausted = false;
        true
    }

    pub fn clear_selection(&mut self) {
        self.selected_channel = None;
        self.pending = None;
        self.exhausted = false;
    }

    pub fn try_load_latest(
        &mut self,
        rest: &RestHandle,
    ) -> Result<HistoryLoadStatus, RestSubmitError> {
        if self.pending.is_some() {
            return Ok(HistoryLoadStatus::Busy);
        }

        let Some(channel_id) = self.selected_channel.as_deref() else {
            return Ok(HistoryLoadStatus::NoSelection);
        };

        let request_id = rest.try_fetch_messages(channel_id, self.page_size)?;
        self.pending = Some(PendingHistory {
            request_id,
            channel_id: Box::<str>::from(channel_id),
            requested: self.page_size,
            before: None,
        });
        self.exhausted = false;
        Ok(HistoryLoadStatus::Submitted(request_id))
    }

    pub fn try_load_older(
        &mut self,
        state: &FrontendState,
        rest: &RestHandle,
    ) -> Result<HistoryLoadStatus, RestSubmitError> {
        if self.pending.is_some() {
            return Ok(HistoryLoadStatus::Busy);
        }
        if self.exhausted {
            return Ok(HistoryLoadStatus::Exhausted);
        }

        let Some(channel_id) = self.selected_channel.as_deref() else {
            return Ok(HistoryLoadStatus::NoSelection);
        };
        let Some(oldest_id) = state
            .messages(channel_id)
            .next()
            .map(|message| message.id.as_ref())
        else {
            return Ok(HistoryLoadStatus::NoAnchor);
        };

        let request_id = rest.try_fetch_messages_before(channel_id, oldest_id, self.page_size)?;
        self.pending = Some(PendingHistory {
            request_id,
            channel_id: Box::<str>::from(channel_id),
            requested: self.page_size,
            before: Some(Box::<str>::from(oldest_id)),
        });
        Ok(HistoryLoadStatus::Submitted(request_id))
    }

    pub fn observe_rest(&mut self, event: &RestEvent) -> HistoryCompletion {
        let Some(pending) = self.pending.as_ref() else {
            return HistoryCompletion::Ignored;
        };

        match event {
            RestEvent::MessagesFetched {
                request_id,
                channel_id,
                messages,
            } if *request_id == pending.request_id
                && channel_id.as_ref() == pending.channel_id.as_ref() =>
            {
                let exhausted = messages.len() < usize::from(pending.requested);
                self.exhausted = exhausted;
                self.pending = None;
                HistoryCompletion::Loaded {
                    count: messages.len(),
                    exhausted,
                }
            }
            RestEvent::Failed {
                request_id,
                operation: RestOperation::FetchMessages,
                retryable,
                ..
            } if *request_id == pending.request_id => {
                let retryable = *retryable;
                self.pending = None;
                HistoryCompletion::Failed { retryable }
            }
            _ => HistoryCompletion::Ignored,
        }
    }

    pub fn pending_before(&self) -> Option<&str> {
        self.pending
            .as_ref()
            .and_then(|pending| pending.before.as_deref())
    }
}

impl Default for HistoryPager {
    fn default() -> Self {
        Self::new(50)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::{FrontendEvent, FrontendMessage, RestDispatcher, RestMessage, RestOperation};

    use super::*;

    fn rest() -> RestHandle {
        let (handle, _worker) = RestDispatcher::new("test-token", 8, 8).unwrap();
        handle
    }

    fn state_with_messages() -> FrontendState {
        let mut state = FrontendState::new(4, 16);
        for id in ["100", "110", "120"] {
            state.apply(Arc::new(FrontendEvent::Message(FrontendMessage {
                id: Box::<str>::from(id),
                channel_id: Box::<str>::from("22"),
                author_username: Box::<str>::from("tester"),
                content: Box::<str>::from(id),
            })));
        }
        state
    }

    #[test]
    fn page_size_is_clamped_to_discord_bounds() {
        assert_eq!(HistoryPager::new(0).page_size(), 1);
        assert_eq!(HistoryPager::new(50).page_size(), 50);
        assert_eq!(HistoryPager::new(255).page_size(), 100);
    }

    #[test]
    fn selection_change_resets_request_and_exhaustion_state() {
        let rest = rest();
        let mut pager = HistoryPager::new(25);
        assert!(pager.select_channel("22"));
        let first = pager.try_load_latest(&rest).unwrap();
        assert!(matches!(first, HistoryLoadStatus::Submitted(_)));
        assert!(pager.pending_request_id().is_some());

        assert!(pager.select_channel("23"));
        assert_eq!(pager.selected_channel(), Some("23"));
        assert_eq!(pager.pending_request_id(), None);
        assert!(!pager.is_exhausted());
        assert!(!pager.select_channel("23"));
    }

    #[test]
    fn one_history_request_is_allowed_at_a_time() {
        let rest = rest();
        let mut pager = HistoryPager::new(25);
        pager.select_channel("22");
        assert!(matches!(
            pager.try_load_latest(&rest).unwrap(),
            HistoryLoadStatus::Submitted(_)
        ));
        assert_eq!(
            pager.try_load_latest(&rest).unwrap(),
            HistoryLoadStatus::Busy
        );
    }

    #[test]
    fn older_page_uses_the_oldest_cached_message_as_anchor() {
        let rest = rest();
        let state = state_with_messages();
        let mut pager = HistoryPager::new(25);
        pager.select_channel("22");

        assert!(matches!(
            pager.try_load_older(&state, &rest).unwrap(),
            HistoryLoadStatus::Submitted(_)
        ));
        assert_eq!(pager.pending_before(), Some("100"));
    }

    #[test]
    fn older_page_requires_a_cached_anchor() {
        let rest = rest();
        let state = FrontendState::new(4, 16);
        let mut pager = HistoryPager::new(25);
        pager.select_channel("22");

        assert_eq!(
            pager.try_load_older(&state, &rest).unwrap(),
            HistoryLoadStatus::NoAnchor
        );
    }

    #[test]
    fn short_page_marks_history_exhausted() {
        let rest = rest();
        let mut pager = HistoryPager::new(3);
        pager.select_channel("22");
        let request_id = match pager.try_load_latest(&rest).unwrap() {
            HistoryLoadStatus::Submitted(request_id) => request_id,
            other => panic!("unexpected status: {other:?}"),
        };

        let completion = pager.observe_rest(&RestEvent::MessagesFetched {
            request_id,
            channel_id: Box::<str>::from("22"),
            messages: vec![RestMessage {
                id: Box::<str>::from("100"),
                channel_id: Box::<str>::from("22"),
                author_username: Some(Box::<str>::from("tester")),
                content: Box::<str>::from("one"),
            }],
        });

        assert_eq!(
            completion,
            HistoryCompletion::Loaded {
                count: 1,
                exhausted: true
            }
        );
        assert!(pager.is_exhausted());
        assert_eq!(pager.pending_request_id(), None);
    }

    #[test]
    fn full_page_keeps_backward_pagination_available() {
        let rest = rest();
        let mut pager = HistoryPager::new(2);
        pager.select_channel("22");
        let request_id = match pager.try_load_latest(&rest).unwrap() {
            HistoryLoadStatus::Submitted(request_id) => request_id,
            other => panic!("unexpected status: {other:?}"),
        };

        let messages = ["100", "101"]
            .into_iter()
            .map(|id| RestMessage {
                id: Box::<str>::from(id),
                channel_id: Box::<str>::from("22"),
                author_username: Some(Box::<str>::from("tester")),
                content: Box::<str>::from(id),
            })
            .collect();
        let completion = pager.observe_rest(&RestEvent::MessagesFetched {
            request_id,
            channel_id: Box::<str>::from("22"),
            messages,
        });

        assert_eq!(
            completion,
            HistoryCompletion::Loaded {
                count: 2,
                exhausted: false
            }
        );
        assert!(!pager.is_exhausted());
    }

    #[test]
    fn matching_failure_releases_the_request_for_retry() {
        let rest = rest();
        let mut pager = HistoryPager::new(25);
        pager.select_channel("22");
        let request_id = match pager.try_load_latest(&rest).unwrap() {
            HistoryLoadStatus::Submitted(request_id) => request_id,
            other => panic!("unexpected status: {other:?}"),
        };

        let completion = pager.observe_rest(&RestEvent::Failed {
            request_id,
            operation: RestOperation::FetchMessages,
            status: Some(500),
            retryable: true,
            message: Box::<str>::from("server error"),
        });

        assert_eq!(completion, HistoryCompletion::Failed { retryable: true });
        assert_eq!(pager.pending_request_id(), None);
        assert!(!pager.is_exhausted());
        assert!(matches!(
            pager.try_load_latest(&rest).unwrap(),
            HistoryLoadStatus::Submitted(_)
        ));
    }

    #[test]
    fn unrelated_rest_events_are_ignored() {
        let rest = rest();
        let mut pager = HistoryPager::new(25);
        pager.select_channel("22");
        let request_id = match pager.try_load_latest(&rest).unwrap() {
            HistoryLoadStatus::Submitted(request_id) => request_id,
            other => panic!("unexpected status: {other:?}"),
        };

        let completion = pager.observe_rest(&RestEvent::Failed {
            request_id: request_id.wrapping_add(1),
            operation: RestOperation::FetchMessages,
            status: Some(500),
            retryable: true,
            message: Box::<str>::from("other request"),
        });

        assert_eq!(completion, HistoryCompletion::Ignored);
        assert_eq!(pager.pending_request_id(), Some(request_id));
    }
}
