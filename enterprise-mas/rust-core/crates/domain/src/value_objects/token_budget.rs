//! Token budget for LLM token consumption control.

use mas_common::error::AppError;
use mas_common::result::Result;
use serde::{Deserialize, Serialize};

/// Tracks a token allowance and its consumption.
///
/// `max_tokens == 0` means *unmetered* (unlimited within platform hard caps);
/// enforcement of hard caps happens in the quota layer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TokenBudget {
    max_tokens: u64,
    consumed: u64,
}

impl TokenBudget {
    /// Creates a budget of at most `max_tokens`.
    pub fn new(max_tokens: u64) -> Result<Self> {
        if max_tokens > mas_common::constants::MAX_TOKENS_PER_EXECUTION {
            return Err(AppError::invalid_field(
                "max_tokens",
                "out_of_range",
                format!(
                    "token budget exceeds hard platform cap of {}",
                    mas_common::constants::MAX_TOKENS_PER_EXECUTION
                ),
            ));
        }
        Ok(Self {
            max_tokens,
            consumed: 0,
        })
    }

    /// Unlimited (unmetered) budget — still bound by quota-layer hard caps.
    #[must_use]
    pub const fn unlimited() -> Self {
        Self {
            max_tokens: 0,
            consumed: 0,
        }
    }

    #[must_use]
    pub const fn max_tokens(&self) -> u64 {
        self.max_tokens
    }

    #[must_use]
    pub const fn consumed(&self) -> u64 {
        self.consumed
    }

    /// Whether this budget meters consumption.
    #[must_use]
    pub const fn is_metered(&self) -> bool {
        self.max_tokens > 0
    }

    /// Remaining allowance; `u64::MAX` when unmetered.
    #[must_use]
    pub const fn remaining(&self) -> u64 {
        if self.is_metered() {
            self.max_tokens - self.consumed
        } else {
            u64::MAX
        }
    }

    #[must_use]
    pub const fn is_exhausted(&self) -> bool {
        self.is_metered() && self.consumed >= self.max_tokens
    }

    /// Consumes `amount` tokens. Fails with `RateLimited` when the metered
    /// budget would be exceeded (consuming nothing in that case).
    pub fn consume(&mut self, amount: u64) -> Result<()> {
        if !self.is_metered() {
            self.consumed = self.consumed.saturating_add(amount);
            return Ok(());
        }
        let next = self
            .consumed
            .checked_add(amount)
            .ok_or_else(|| AppError::rate_limited("token consumption counter overflow"))?;
        if next > self.max_tokens {
            return Err(AppError::rate_limited(format!(
                "token budget exhausted ({}/{} used, requested {} more)",
                self.consumed, self.max_tokens, amount
            )));
        }
        self.consumed = next;
        Ok(())
    }

    /// Consumes up to `amount`, returning how much was actually granted.
    pub fn consume_up_to(&mut self, amount: u64) -> u64 {
        let grant = amount.min(self.remaining());
        // `remaining()` guarantees this cannot overflow.
        self.consumed = self.consumed.saturating_add(grant);
        grant
    }
}
