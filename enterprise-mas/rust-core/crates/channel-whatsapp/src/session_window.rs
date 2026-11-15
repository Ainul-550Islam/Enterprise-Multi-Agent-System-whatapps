/// 24-hour customer service window management for WhatsApp Cloud API.
///
/// Meta enforces a "customer service window" of 24 hours from the last
/// inbound message. Within the window, free-form messages can be sent.
/// Outside the window, only approved templates may be sent.
///
/// This crate provides the window logic; the application layer decides
/// whether to allow free-form or require a template.
#[derive(Clone, Debug, PartialEq)]
pub struct CustomerWindow {
    /// The window opens at this time (epoch-ms, last inbound received)
    opened_at: i64,
    /// The window closes at this time (epoch-ms) — 24h after opened_at
    closes_at: i64,
}

impl CustomerWindow {
    /// Create a new window timestamped now (last inbound received at now).
    /// The window is valid for 24 hours from now.
    pub fn now() -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|_| {}) 
            .unwrap()
            .as_millis() as i64;
        CustomerWindow {
            opened_at: now,
            closes_at: now + (24 * 60 * 60 * 1000), // 24h in ms
        }
    }

    /// The timestamp when the window was opened (last inbound).
    pub fn opened_at(&self) -> i64 {
        self.opened_at
    }

    /// The timestamp when the window closes (24h after open).
    pub fn closes_at(&self) -> i64 {
        self.closes_at
    }

    /// Check whether the window is currently open (i.e. now < closes_at).
    pub fn is_open(&self, now: i64) -> bool {
        now < self.closes_at
    }

    /// Check whether the window is currently open (uses current time).
    pub fn is_open_now(&self) -> bool {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|_| {})
            .unwrap()
            .as_millis() as i64;
        self.is_open(now)
    }

    /// Calculate minutes remaining in the window.
    pub fn minutes_remaining(&self, now: i64) -> Option<u64> {
        let remaining = self.closes_at.saturating_sub(now);
        if remaining <= 0 {
            None
        } else {
            Some((remaining / 60_000) as u64) // convert ms to minutes
        }
    }

    /// Calculate minutes remaining (uses current time).
    pub fn minutes_remaining_now(&self) -> Option<u64> {
        self.minutes_remaining(std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|_| {})
            .unwrap()
            .as_millis() as i64)
    }

    /// Determine whether a free-form message is allowed.
    /// Free-form is only allowed within the 24h window.
    pub fn allows_free_form(&self, now: i64) -> bool {
        self.is_open(now)
    }

    /// Determine whether a free-form message is allowed (now).
    pub fn allows_free_form_now(&self) -> bool {
        self.allows_free_form_now()
    }

    /// Determine whether only approved templates are allowed.
    /// This is the case when the window is closed.
    pub fn requires_template(&self, now: i64) -> bool {
        !self.is_open(now)
    }

    /// Whether only approved templates are required (now).
    pub fn requires_template_now(&self) -> bool {
        self.requires_template_now()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_window_now_is_open() {
        let window = CustomerWindow::now();
        let now = window.opened_at();
        assert!(window.is_open(now));
    }

    #[test]
    fn test_window_closes_after_24h() {
        let window = CustomerWindow::now();
        // Window should be open at now, closed at now + 25h
        let later = window.opened_at() + (25 * 60 * 60 * 1000);
        assert!(!window.is_open(later));
    }

    #[test]
    fn test_minutes_remaining() {
        let window = CustomerWindow::now();
        let now = window.opened_at();
        let mins = window.minutes_remaining(now);
        assert!(mins.is_some());
        assert_eq!(mins.unwrap(), 1440); // 24h = 1440 minutes
    }

    #[test]
    fn test_free_form_within_window() {
        let window = CustomerWindow::now();
        let now = window.opened_at();
        assert!(window.allows_free_form(now));
        assert!(!window.requires_template(now));
    }

    #[test]
    fn test_requires_template_outside_window() {
        let window = CustomerWindow::now();
        let later = window.opened_at() + (25 * 60 * 60 * 1000);
        assert!(!window.allows_free_form(later));
        assert!(window.requires_template(later));
    }
}