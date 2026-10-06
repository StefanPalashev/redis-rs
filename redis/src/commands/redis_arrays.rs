//! Defines types to use with the array commands (Redis 8.8+).

use crate::{RedisWrite, ToRedisArgs};
use std::num::NonZeroUsize;

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

/// Optional parameters for the `ARSCAN` command.
///
/// # Example
/// ```rust,no_run
/// use redis::{Commands, RedisResult, redis_arrays::*};
/// use std::num::NonZeroUsize;
/// fn scan_page(con: &mut redis::Connection, key: &str) -> RedisResult<Vec<(usize, String)>> {
///     let opts = ArrayScanOptions::default().set_limit(NonZeroUsize::new(100).unwrap());
///     con.arscan_options(key, 0, 1000, &opts)
/// }
/// ```
#[derive(Clone, Debug, Default)]
#[non_exhaustive]
pub struct ArrayScanOptions {
    /// Cap the number of pairs returned.
    limit: Option<NonZeroUsize>,
}

impl ArrayScanOptions {
    /// Return at most `limit` pairs (`LIMIT`).
    ///
    /// The server requires a positive limit, which [`NonZeroUsize`] enforces at compile time.
    pub fn set_limit(mut self, limit: NonZeroUsize) -> Self {
        self.limit = Some(limit);
        self
    }
}

impl ToRedisArgs for ArrayScanOptions {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        if let Some(limit) = self.limit {
            out.write_arg(b"LIMIT");
            limit.write_redis_args(out);
        }
    }
}

/// An aggregate operation for the `AROP` command.
///
/// The reply type depends on the operation, which is why `arop` returns a generic value the caller interprets:
/// - [`Sum`](ArrayAggregateOp::Sum) is an integer or float depending on the data.
/// - [`Min`](ArrayAggregateOp::Min) / [`Max`](ArrayAggregateOp::Max) return the element value or Nil when the range holds no numeric elements.
/// - [`And`](ArrayAggregateOp::And) / [`Or`](ArrayAggregateOp::Or) / [`Xor`](ArrayAggregateOp::Xor) are integer bitwise reductions.
/// - [`Match`](ArrayAggregateOp::Match) and [`Used`](ArrayAggregateOp::Used) return integer counts.
#[derive(Clone, Copy, Debug)]
#[non_exhaustive]
pub enum ArrayAggregateOp<'a> {
    /// Sum of the numeric elements in the range (`SUM`).
    Sum,
    /// Minimum numeric element in the range (`MIN`).
    Min,
    /// Maximum numeric element in the range (`MAX`).
    Max,
    /// Bitwise AND of the elements in the range (`AND`).
    And,
    /// Bitwise OR of the elements in the range (`OR`).
    Or,
    /// Bitwise XOR of the elements in the range (`XOR`).
    Xor,
    /// Count of elements in the range exactly equal to the given value (`MATCH`).
    ///
    /// This comparison is **case-sensitive**.
    Match(&'a str),
    /// Count of populated (non-empty) slots in the range (`USED`).
    Used,
}

impl ToRedisArgs for ArrayAggregateOp<'_> {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        match self {
            ArrayAggregateOp::Sum => out.write_arg(b"SUM"),
            ArrayAggregateOp::Min => out.write_arg(b"MIN"),
            ArrayAggregateOp::Max => out.write_arg(b"MAX"),
            ArrayAggregateOp::And => out.write_arg(b"AND"),
            ArrayAggregateOp::Or => out.write_arg(b"OR"),
            ArrayAggregateOp::Xor => out.write_arg(b"XOR"),
            ArrayAggregateOp::Match(value) => {
                out.write_arg(b"MATCH");
                out.write_arg(value.as_bytes());
            }
            ArrayAggregateOp::Used => out.write_arg(b"USED"),
        }
    }
}

/// Optional parameters for the `ARINFO` command.
///
/// # Example
/// ```rust,no_run
/// use redis::{Commands, RedisResult, Value, redis_arrays::*};
/// use std::collections::HashMap;
/// fn full_info(con: &mut redis::Connection, key: &str) -> RedisResult<HashMap<String, Value>> {
///     let opts = ArrayInfoOptions::default().set_full(true);
///     con.arinfo(key, &opts)
/// }
/// ```
#[derive(Clone, Debug, Default)]
#[non_exhaustive]
pub struct ArrayInfoOptions {
    /// Include the per-slice statistics in the reply.
    full: bool,
}

impl ArrayInfoOptions {
    /// Include additional per-slice statistics in the reply (`FULL`).
    ///
    /// Collecting them costs O(N) in the size of the array.
    pub fn set_full(mut self, full: bool) -> Self {
        self.full = full;
        self
    }
}

impl ToRedisArgs for ArrayInfoOptions {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        if self.full {
            out.write_arg(b"FULL");
        }
    }
}
