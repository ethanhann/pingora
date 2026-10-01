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

//! The time of the next tick of each slot.

use crate::runtime::pool::events::{GuestAddress, SlotIndex};
use std::collections::HashMap;
use std::time::{Duration, Instant};

/// The next tick of each slot whose guest set a tick period.
///
/// A tick is due one period after the last tick ended, as in Envoy, so ticks never overlap and
/// a late tick is not repeated.
#[derive(Default)]
pub(super) struct TickSchedule {
    next_ticks: HashMap<SlotIndex, (GuestAddress, Instant)>,
}

impl TickSchedule {
    /// Set the tick period of the guest at `address`.
    ///
    /// `None` stops the ticks of that guest.
    pub(super) fn set_period(
        &mut self,
        address: GuestAddress,
        period: Option<Duration>,
        now: Instant,
    ) {
        let key = address.slot;
        match period {
            Some(period) => {
                self.next_ticks.insert(key, (address, now + period));
            }
            None => {
                if self
                    .next_ticks
                    .get(&key)
                    .is_some_and(|(a, _)| *a == address)
                {
                    self.next_ticks.remove(&key);
                }
            }
        }
    }

    /// Return whether `slot` has a next tick.
    pub(super) fn has_next_tick(&self, slot: SlotIndex) -> bool {
        self.next_ticks.contains_key(&slot)
    }

    /// Return the time of the next tick.
    pub(super) fn next_due(&self) -> Option<Instant> {
        self.next_ticks.values().map(|(_, due)| *due).min()
    }

    /// Remove and return the slots whose tick is due at `now`.
    ///
    /// A slot gets its next tick when [TickSchedule::set_period] runs again after the tick.
    pub(super) fn take_due(&mut self, now: Instant) -> Vec<GuestAddress> {
        let due: Vec<_> = self
            .next_ticks
            .iter()
            .filter(|(_, (_, at))| *at <= now)
            .map(|(key, (address, _))| (*key, *address))
            .collect();
        for (key, _) in &due {
            self.next_ticks.remove(key);
        }
        due.into_iter().map(|(_, address)| address).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proxy_wasm_host::abi::v0_2_1::GuestId;

    fn address(slot: usize, guest: GuestId) -> GuestAddress {
        GuestAddress {
            slot: SlotIndex {
                pool_index: 0,
                slot_index: slot,
            },
            guest,
        }
    }

    #[test]
    fn a_tick_is_due_one_period_after_the_period_was_set() {
        let mut schedule = TickSchedule::default();
        let start = Instant::now();
        let address = address(0, GuestId::next());
        schedule.set_period(address, Some(Duration::from_millis(100)), start);

        let early = schedule.take_due(start + Duration::from_millis(99));
        let due = schedule.take_due(start + Duration::from_millis(100));

        assert!(early.is_empty());
        assert_eq!(due, vec![address]);
        assert_eq!(schedule.next_due(), None);
    }

    #[test]
    fn a_period_of_zero_stops_the_ticks() {
        let mut schedule = TickSchedule::default();
        let start = Instant::now();
        let address = address(0, GuestId::next());
        schedule.set_period(address, Some(Duration::from_millis(100)), start);

        schedule.set_period(address, None, start);

        assert_eq!(schedule.next_due(), None);
    }

    #[test]
    fn a_period_of_zero_from_a_replaced_guest_keeps_the_ticks_of_the_new_guest() {
        let mut schedule = TickSchedule::default();
        let start = Instant::now();
        let new_guest = address(0, GuestId::next());
        schedule.set_period(new_guest, Some(Duration::from_millis(100)), start);

        schedule.set_period(address(0, GuestId::next()), None, start);

        assert_eq!(
            schedule.next_due(),
            Some(start + Duration::from_millis(100))
        );
    }

    #[test]
    fn the_next_due_tick_is_the_earliest_of_all_slots() {
        let mut schedule = TickSchedule::default();
        let start = Instant::now();
        schedule.set_period(
            address(0, GuestId::next()),
            Some(Duration::from_secs(1)),
            start,
        );
        schedule.set_period(
            address(1, GuestId::next()),
            Some(Duration::from_millis(10)),
            start,
        );

        let next = schedule.next_due();

        assert_eq!(next, Some(start + Duration::from_millis(10)));
    }
}
