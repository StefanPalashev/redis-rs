//! Provides a type-safe way to generate [FT.DROPINDEX](https://redis.io/docs/latest/commands/ft.dropindex/) commands programmatically.
//!
//! # Examples
//!
//! ## Drop index only
//!
//! ```rust
//! use redis::search::*;
//!
//! let ft_dropindex = FtDropIndexCommand::new("index")
//!     .into_cmd();
//! ```
//!
//! ## Drop index and delete documents
//!
//! ```rust
//! use redis::search::*;
//!
//! let ft_dropindex = FtDropIndexCommand::new("index")
//!     .delete_documents()
//!     .into_cmd();
//! ```
use crate::Cmd;

/// FT.DROPINDEX command builder.
///
/// # Example
/// ```rust
/// use redis::search::*;
///
/// let cmd = FtDropIndexCommand::new("index")
///     .delete_documents()
///     .into_cmd();
/// ```
pub struct FtDropIndexCommand {
    index: String,
    delete_documents: bool,
}

impl FtDropIndexCommand {
    /// Create a new FT.DROPINDEX command for the given index
    pub fn new<S: Into<String>>(index: S) -> Self {
        Self {
            index: index.into(),
            delete_documents: false,
        }
    }

    /// Set the DD option to delete the documents as well
    pub fn delete_documents(mut self) -> Self {
        self.delete_documents = true;
        self
    }

    /// Consume the builder and convert it into a `redis::Cmd`.
    pub fn into_cmd(self) -> Cmd {
        let mut cmd = crate::cmd("FT.DROPINDEX");
        cmd.arg(&self.index);
        if self.delete_documents {
            cmd.arg("DD");
        }

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
                Arg::Cursor => panic!("Cursor not expected in FT.DROPINDEX command"),
            })
            .map(|arg| String::from_utf8_lossy(&arg).to_string())
            .collect::<Vec<_>>()
            .join(" ")
    }
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
