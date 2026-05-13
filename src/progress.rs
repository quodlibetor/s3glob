use std::fmt;
use std::sync::OnceLock;
use std::time::Duration;

use indicatif::{MultiProgress, ProgressBar, ProgressDrawTarget, ProgressStyle};

use crate::messaging::{self, MessageLevel};

pub(crate) struct Progress {
    multi: Option<MultiProgress>,
}

static PROGRESS: OnceLock<Progress> = OnceLock::new();

pub(crate) fn init(level: MessageLevel) {
    let multi = if level == MessageLevel::Normal {
        Some(MultiProgress::with_draw_target(
            ProgressDrawTarget::stderr_with_hz(15),
        ))
    } else {
        None
    };
    let _ = PROGRESS.set(Progress { multi });
}

pub(crate) fn get() -> &'static Progress {
    PROGRESS.get_or_init(|| Progress { multi: None })
}

/// RAII guard that calls `finish_and_clear()` on the contained bar when
/// dropped. Use for bars whose owning scope has `?`-propagation paths so
/// an early-return error doesn't leave a half-drawn spinner on screen.
pub(crate) struct ClearOnDrop<'a>(pub &'a ProgressBar);

impl Drop for ClearOnDrop<'_> {
    fn drop(&mut self) {
        self.0.finish_and_clear();
    }
}

impl Progress {
    /// Add a determinate progress bar (e.g. for byte counts with a known total).
    pub(crate) fn bar(&self, style: ProgressStyle) -> ProgressBar {
        match &self.multi {
            Some(m) => m.add(ProgressBar::new(0).with_style(style)),
            None => ProgressBar::hidden(),
        }
    }

    /// Add a spinner-style progress bar (e.g. for open-ended counters).
    pub(crate) fn spinner(&self, style: ProgressStyle) -> ProgressBar {
        match &self.multi {
            Some(m) => {
                let pb = m.add(ProgressBar::new_spinner().with_style(style));
                pb.enable_steady_tick(Duration::from_millis(100));
                pb
            }
            None => ProgressBar::hidden(),
        }
    }

    /// Print a discrete progress message to stderr (suppressed by `-q`).
    ///
    /// `MultiProgress::println` is silent when the draw target is hidden
    /// (non-TTY stderr) and unreliable after all bars have been cleared,
    /// so we route around it and print directly. `multi.suspend` keeps the
    /// active bars from corrupting the line in the common TTY case.
    pub(crate) fn log(&self, msg: impl fmt::Display) {
        if !messaging::louder_than(MessageLevel::Quiet) {
            return;
        }
        match &self.multi {
            Some(m) => m.suspend(|| eprintln!("{msg}")),
            None => eprintln!("{msg}"),
        }
    }
}

/// Print a discrete message above any active progress bars.
///
/// Suppressed when `--quiet` or `--quiet --quiet` is set (matches the
/// pre-indicatif behavior of `progressln!`).
#[macro_export]
macro_rules! progressln {
    ($($args:tt)*) => {
        $crate::progress::get().log(format_args!($($args)*))
    };
}

/// Styles for the bars used by `s3glob`. Kept in one place so the
/// `{spinner}` glyph, padding, and tick chars stay consistent.
const TICK_CHARS: &str = "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏ ";

pub(crate) fn prefix_spinner_style() -> ProgressStyle {
    ProgressStyle::with_template("{spinner:.green} discovering prefixes: {pos:>6}")
        .expect("static template")
        .tick_chars(TICK_CHARS)
}

pub(crate) fn matches_spinner_style() -> ProgressStyle {
    ProgressStyle::with_template(
        "{spinner:.green} matches/total {msg} prefixes completed/total {prefix}",
    )
    .expect("static template")
    .tick_chars(TICK_CHARS)
}

pub(crate) fn downloads_count_style() -> ProgressStyle {
    ProgressStyle::with_template(
        "{spinner:.green} downloaded {pos}/{len} objects [{elapsed_precise}]",
    )
    .expect("static template")
    .tick_chars(TICK_CHARS)
}

pub(crate) fn downloads_bytes_style() -> ProgressStyle {
    ProgressStyle::with_template("  {bytes:>10} transferred ({binary_bytes_per_sec})")
        .expect("static template")
}
