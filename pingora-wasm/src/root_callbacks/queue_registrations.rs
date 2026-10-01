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

//! Which root receives `proxy_on_queue_ready` for a queue item.

use crate::runtime::pool::events::GuestAddress;
use proxy_wasm_host::abi::v0_2_1::{ContextId, QueueId};
use std::collections::HashMap;

/// The root of a guest that registered a queue.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Registrant {
    pub(super) address: GuestAddress,
    pub(super) root: ContextId,
}

/// The registrants of each queue, in the order they registered, and the number of items that
/// arrived while no live guest had the queue registered.
///
/// The last registrant receives each item, as in Envoy. Each slot has at most one entry for a
/// queue, and a new registration of the slot moves it to the end.
#[derive(Default)]
pub(super) struct QueueRegistrations {
    registrants: HashMap<QueueId, Vec<Registrant>>,
    pending_items: HashMap<QueueId, usize>,
}

impl QueueRegistrations {
    /// Record that `root` of the guest at `address` registered `queue`.
    ///
    /// Return the number of items that arrived while no live guest had the queue registered,
    /// which the new registrant now receives.
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

    /// Return the registrant that receives the next item of `queue`.
    pub(super) fn last_registrant(&self, queue: QueueId) -> Option<Registrant> {
        self.registrants.get(&queue).and_then(|r| r.last()).copied()
    }

    /// Remove a registrant whose guest left its slot.
    pub(super) fn remove(&mut self, queue: QueueId, registrant: Registrant) {
        if let Some(registrants) = self.registrants.get_mut(&queue) {
            registrants.retain(|r| *r != registrant);
        }
    }

    /// Keep an item of `queue` for the next registrant.
    pub(super) fn keep_pending(&mut self, queue: QueueId) {
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
                slot: SlotIndex { pool, slot },
                guest: GuestId::next(),
            },
            root: ContextId::try_from(1).unwrap(),
        }
    }

    fn queue() -> QueueId {
        QueueId::try_from(1).unwrap()
    }

    fn register(registrations: &mut QueueRegistrations, registrant: Registrant) -> usize {
        registrations.register(queue(), registrant.address, registrant.root)
    }

    #[test]
    fn the_last_registrant_receives_an_item_across_plugins_with_one_vm_id() {
        let mut registrations = QueueRegistrations::default();
        let first = registrant(0, 0);
        let second_plugin = registrant(1, 0);
        register(&mut registrations, first);
        register(&mut registrations, second_plugin);

        let receiver = registrations.last_registrant(queue());

        assert_eq!(receiver, Some(second_plugin));
    }

    #[test]
    fn a_new_registration_of_a_slot_moves_it_to_the_end_once() {
        let mut registrations = QueueRegistrations::default();
        let slot_0 = registrant(0, 0);
        let slot_1 = registrant(0, 1);
        register(&mut registrations, slot_0);
        register(&mut registrations, slot_1);

        register(&mut registrations, slot_0);

        assert_eq!(registrations.last_registrant(queue()), Some(slot_0));
        registrations.remove(queue(), slot_0);
        assert_eq!(registrations.last_registrant(queue()), Some(slot_1));
    }

    #[test]
    fn the_registrant_before_receives_an_item_when_the_last_left() {
        let mut registrations = QueueRegistrations::default();
        let first = registrant(0, 0);
        let replaced = registrant(0, 1);
        register(&mut registrations, first);
        register(&mut registrations, replaced);

        registrations.remove(queue(), replaced);

        assert_eq!(registrations.last_registrant(queue()), Some(first));
    }

    #[test]
    fn an_item_with_no_live_registrant_goes_to_the_next_registrant() {
        let mut registrations = QueueRegistrations::default();
        registrations.keep_pending(queue());
        registrations.keep_pending(queue());

        let waiting = register(&mut registrations, registrant(0, 0));

        assert_eq!(waiting, 2);
        assert_eq!(register(&mut registrations, registrant(0, 1)), 0);
    }
}
