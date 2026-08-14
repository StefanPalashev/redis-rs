//! Defines types to use with the array commands (Redis 8.8+).

use crate::{RedisWrite, ToRedisArgs};

/// Optional parameters for the `ARLASTITEMS` command.
///
/// # Example
/// ```rust,no_run
/// use redis::{Commands, RedisResult, redis_arrays::*};
/// fn newest_first(con: &mut redis::Connection, key: &str) -> RedisResult<Vec<Option<String>>> {
///     let opts = ArrayLastItemsOptions::default().set_rev(true);
///     con.arlastitems(key, 10, &opts)
/// }
/// ```
#[derive(Clone, Debug, Default)]
#[non_exhaustive]
pub struct ArrayLastItemsOptions {
    /// Return the most recent elements in reverse index order.
    rev: bool,
}

impl ArrayLastItemsOptions {
    /// Return the most recent elements in reverse index order (`REV`), newest first.
    pub fn set_rev(mut self, rev: bool) -> Self {
        self.rev = rev;
        self
    }
}

impl ToRedisArgs for ArrayLastItemsOptions {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        if self.rev {
            out.write_arg(b"REV");
        }
    }
}
