// Copyright 2026 Cloudflare, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Shared queue registrations
//!
//! Tracks which root contexts registered each queue, and so which one gets
//! `proxy_on_queue_ready` when an item is enqueued.

use crate::runtime::pool::events::GuestAddress;
use proxy_wasm_host::abi::v0_2_1::{ContextId, QueueId};
use std::collections::HashMap;

/// A root context that registered a queue, and the guest it belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Registrant {
    pub(super) address: GuestAddress,
    pub(super) root: ContextId,
}

/// Per-queue registrants in registration order, plus a count of items still waiting for one.
///
/// Each item is delivered to the most recent registrant. A slot appears at most once per queue,
/// and registering again from the same slot moves it to the end. Items enqueued while a queue has
/// no live registrant are counted as pending.
#[derive(Default)]
pub(super) struct QueueRegistrations {
    registrants: HashMap<QueueId, Vec<Registrant>>,
    pending_items: HashMap<QueueId, usize>,
}

impl QueueRegistrations {
    /// Register `root` of the guest at `address` as the latest registrant of `queue`.
    ///
    /// Any earlier registration from the same slot is replaced. Returns the number of pending
    /// items, which this registrant now receives, and resets that count.
    pub(super) fn register(
        &mut self,
        queue: QueueId,
        address: GuestAddress,
        root: ContextId,
    ) -> usize {
        let registrants = self.registrants.entry(queue).or_default();
        registrants.retain(|r| r.address.slot != address.slot);
        registrants.push(Registrant { address, root });
        self.pending_items.remove(&queue).unwrap_or(0)
    }

    /// Return the registrant the next item of `queue` should be delivered to.
    pub(super) fn last_registrant(&self, queue: QueueId) -> Option<Registrant> {
        self.registrants.get(&queue).and_then(|r| r.last()).copied()
    }

    /// Remove a registrant whose guest is no longer in its slot.
    pub(super) fn remove(&mut self, queue: QueueId, registrant: Registrant) {
        if let Some(registrants) = self.registrants.get_mut(&queue) {
            registrants.retain(|r| *r != registrant);
        }
    }

    /// Count an item enqueued on `queue` as pending for the next guest that registers it.
    pub(super) fn add_pending_item(&mut self, queue: QueueId) {
        *self.pending_items.entry(queue).or_default() += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::pool::events::SlotIndex;
    use proxy_wasm_host::abi::v0_2_1::GuestId;

    fn registrant(pool: usize, slot: usize) -> Registrant {
        Registrant {
            address: GuestAddress {
                slot: SlotIndex {
                    pool_index: pool,
                    slot_index: slot,
                },
                guest: GuestId::next(),
            },
            root: ContextId::try_from(1).unwrap(),
        }
    }

    fn queue_1() -> QueueId {
        QueueId::try_from(1).unwrap()
    }

    fn register(registrations: &mut QueueRegistrations, registrant: Registrant) -> usize {
        registrations.register(queue_1(), registrant.address, registrant.root)
    }

    #[test]
    fn reregistering_slot_moves_it_to_end() {
        let mut registrations = QueueRegistrations::default();
        let slot_0 = registrant(0, 0);
        register(&mut registrations, slot_0);
        register(&mut registrations, registrant(0, 1));

        register(&mut registrations, slot_0);

        assert_eq!(registrations.last_registrant(queue_1()), Some(slot_0));
    }

    #[test]
    fn slot_registering_twice_has_one_entry() {
        let mut registrations = QueueRegistrations::default();
        let slot_0 = registrant(0, 0);
        let slot_1 = registrant(0, 1);
        register(&mut registrations, slot_0);
        register(&mut registrations, slot_1);
        register(&mut registrations, slot_0);

        registrations.remove(queue_1(), slot_0);

        assert_eq!(registrations.last_registrant(queue_1()), Some(slot_1));
    }

    #[test]
    fn previous_registrant_takes_over_when_last_is_removed() {
        let mut registrations = QueueRegistrations::default();
        let first = registrant(0, 0);
        let replaced = registrant(0, 1);
        register(&mut registrations, first);
        register(&mut registrations, replaced);

        registrations.remove(queue_1(), replaced);

        assert_eq!(registrations.last_registrant(queue_1()), Some(first));
    }

    #[test]
    fn pending_items_go_to_next_registrant() {
        let mut registrations = QueueRegistrations::default();
        registrations.add_pending_item(queue_1());
        registrations.add_pending_item(queue_1());

        let pending = register(&mut registrations, registrant(0, 0));

        assert_eq!(pending, 2);
    }

    #[test]
    fn pending_items_are_handed_out_once() {
        let mut registrations = QueueRegistrations::default();
        registrations.add_pending_item(queue_1());
        register(&mut registrations, registrant(0, 0));

        let pending = register(&mut registrations, registrant(0, 1));

        assert_eq!(pending, 0);
    }
}
