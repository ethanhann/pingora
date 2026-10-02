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

//! Tick schedule

use crate::runtime::pool::events::{GuestAddress, SlotIndex};
use std::collections::HashMap;
use std::time::{Duration, Instant};

/// When each slot's next `proxy_on_tick` is due, for slots whose guest has set a tick period.
///
/// The next tick is scheduled one period after the previous one finished. Ticks of a slot
/// therefore never overlap, and a tick that ran late is not made up for.
#[derive(Default)]
pub(super) struct TickSchedule {
    next_ticks: HashMap<SlotIndex, (GuestAddress, Instant)>,
}

impl TickSchedule {
    /// Schedule the next tick of the guest at `address` one `period` after `now`.
    ///
    /// A `period` of `None` stops the ticks, unless the slot's entry already belongs to another
    /// guest, in which case it is left alone.
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

    /// Return `true` if a tick is scheduled for `slot`.
    pub(super) fn has_next_tick(&self, slot: SlotIndex) -> bool {
        self.next_ticks.contains_key(&slot)
    }

    /// Return when the earliest scheduled tick is due, if any.
    pub(super) fn next_due(&self) -> Option<Instant> {
        self.next_ticks.values().map(|(_, due)| *due).min()
    }

    /// Remove and return the guests whose tick is due at `now`.
    ///
    /// Nothing is rescheduled here. A slot's next tick is added by calling
    /// [TickSchedule::set_period] again once the tick has run.
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
    fn tick_is_due_one_period_after_period_is_set() {
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
    fn zero_period_stops_ticks() {
        let mut schedule = TickSchedule::default();
        let start = Instant::now();
        let address = address(0, GuestId::next());
        schedule.set_period(address, Some(Duration::from_millis(100)), start);

        schedule.set_period(address, None, start);

        assert_eq!(schedule.next_due(), None);
    }

    #[test]
    fn zero_period_from_replaced_guest_keeps_new_guest_ticks() {
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
    fn next_due_is_earliest_across_slots() {
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
