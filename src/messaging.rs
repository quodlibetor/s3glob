use std::sync::OnceLock;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum MessageLevel {
    VeryQuiet,
    Quiet,
    Normal,
}

pub(crate) static MESSAGE_LEVEL: OnceLock<MessageLevel> = OnceLock::new();

/// A macro that forwards to eprint! if the message level is normal, or
/// does nothing if the message level is quiet
#[macro_export]
macro_rules! progress {
    ($($args:tt)*) => {
        if $crate::messaging::louder_than($crate::messaging::MessageLevel::Quiet) {
            eprint!($($args)*);
        }
    };
}

/// A macro that forwards to eprintln! if the message level is normal, or
/// does nothing if the message level is quiet
#[macro_export]
macro_rules! progressln {
    ($($args:tt)*) => {
        if $crate::messaging::louder_than($crate::messaging::MessageLevel::Quiet) {
            eprintln!($($args)*);
        }
    };
}

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

#[test]
fn test_message_level_ordering() {
    assert2::assert!(MessageLevel::Quiet < MessageLevel::Normal);
    assert2::assert!(MessageLevel::VeryQuiet < MessageLevel::Quiet);
}
