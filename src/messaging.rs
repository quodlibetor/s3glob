use std::sync::OnceLock;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum MessageLevel {
    VeryQuiet,
    Quiet,
    Normal,
}

pub(crate) static MESSAGE_LEVEL: OnceLock<MessageLevel> = OnceLock::new();

/// A macro that forwards to eprintln! if the message level is louder than VeryQuiet
#[macro_export]
macro_rules! message_err {
    ($($args:tt)*) => {
        if $crate::messaging::louder_than($crate::messaging::MessageLevel::VeryQuiet) {
            eprintln!($($args)*);
        }
    };
}

pub(crate) fn louder_than(level: MessageLevel) -> bool {
    let current = *MESSAGE_LEVEL.get_or_init(|| MessageLevel::Normal);
    current > level
}

/// Suppress progress output from the calling test process.
///
/// Intended for proptest cases that would otherwise spam stderr. Safe to
/// call multiple times. The first caller in a process wins — subsequent
/// callers (and any unrelated MESSAGE_LEVEL initializer) silently no-op.
#[cfg(test)]
#[allow(dead_code)]
pub(crate) fn silence_for_tests() {
    let _ = MESSAGE_LEVEL.set(MessageLevel::VeryQuiet);
}

#[test]
fn test_message_level_ordering() {
    assert2::assert!(MessageLevel::Quiet < MessageLevel::Normal);
    assert2::assert!(MessageLevel::VeryQuiet < MessageLevel::Quiet);
}
