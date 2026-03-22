//! Provides a type-safe way to generate [FT.SEARCH](https://redis.io/docs/latest/commands/ft.search/) commands programmatically.
//!
//! # Examples
//!
//! ## Simple search
//!
//! ```rust
//! use redis::search::*;
//!
//! let ft_search = FtSearchCommand::new("products", "laptop")
//!     .into_cmd();
//! ```
//!
//! ## Search with options
//!
//! ```rust
//! use redis::search::*;
//!
//! let ft_search = FtSearchCommand::new("products", "@title:laptop")
//!     .options(
//!         SearchOptions::new()
//!             .withscores()
//!             .limit((0, 10))
//!     )
//!     .into_cmd();
//! ```
use crate::Cmd;
use crate::search::*;

/// FT.SEARCH command builder.
///
/// # Example
/// ```rust
/// use redis::search::*;
///
/// let cmd = FtSearchCommand::new("my_index", "*")
///     .options(
///         SearchOptions::new()
///             .withscores()
///             .limit((0, 10))
///     )
///     .into_cmd();
/// ```
pub struct FtSearchCommand {
    index: String,
    query: String,
    options: Option<SearchOptions>,
}

impl FtSearchCommand {
    /// Create a new FT.SEARCH command for the given index and query
    pub fn new<S: Into<String>>(index: S, query: S) -> Self {
        Self {
            index: index.into(),
            query: query.into(),
            options: None,
        }
    }

    /// Set the options for the command
    pub fn options(mut self, options: SearchOptions) -> Self {
        self.options = Some(options);
        self
    }

    /// Consume the builder and convert it into a `redis::Cmd`.
    pub fn into_cmd(self) -> Cmd {
        let mut cmd = crate::cmd("FT.SEARCH");
        cmd.arg(&self.index);
        cmd.arg(&self.query);
        cmd.arg(self.options);

        cmd
    }

    /// Consume the builder and convert it into a string for testing purposes.
    #[cfg(test)]
    pub(crate) fn into_args(self) -> String {
        use crate::cmd::Arg;
        self.into_cmd()
            .args_iter()
            .map(|arg| match arg {
                Arg::Simple(bytes) => bytes.to_vec(),
                Arg::Cursor => panic!("Cursor not expected in FT.SEARCH command"),
            })
            .map(|arg| {
                let s = String::from_utf8_lossy(&arg).to_string();
                // Quote arguments that contain spaces to make it clear they're single arguments
                if s.contains(' ') {
                    format!("\"{}\"", s)
                } else {
                    s
                }
            })
            .collect::<Vec<_>>()
            .join(" ")
    }
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
