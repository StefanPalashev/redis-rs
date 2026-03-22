//! Defines the types used with the FT.SEARCH command.
//!
//! This module offers type-safe mechanisms for constructing Redis Search queries
//! and configuring the options passed to the command.
//!
//! # Examples
//!
//! ## Simple search
//!
//! ```rust
//! use redis::search::*;
//!
//! let options = SearchOptions::new()
//!     .withscores()
//!     .limit((0, 10));
//! ```
//!
//! ## Search with filters
//!
//! ```rust
//! use redis::search::*;
//! use redis::geo::Unit;
//!
//! let options = SearchOptions::new()
//!     .filter(NumericFilter::new("price", 100.0, 500.0))
//!     .geofilter(GeoFilter::new("location", -122.41, 37.77, 5.0, Unit::Kilometers));
//! ```
use crate::geo::Unit;
use crate::search::SearchLanguage;
use crate::{RedisWrite, ToRedisArgs};
use std::ops::Bound;

/// Query dialect version for FT.SEARCH
///
/// Dialects control query parsing behavior and syntax.
/// See [Redis documentation](https://redis.io/docs/latest/develop/ai/search-and-query/advanced-concepts/dialects/) for details.
#[derive(Clone, Copy, Debug)]
#[non_exhaustive]
pub enum QueryDialect {
    /// Dialect 1 (deprecated)
    ///
    /// **Warning**: This dialect is deprecated. It's recommended to use Dialect 2 even though dialect 1 is the default.
    /// https://redis.io/docs/latest/develop/ai/search-and-query/advanced-concepts/dialects/#dialect-1-deprecated
    #[deprecated(
        since = "1.0.0",
        note = "Dialect 1 is deprecated. Use QueryDialect::Two unless you know exactly what you are doing."
    )]
    One,

    /// Dialect 2
    ///
    /// This is the recommended dialect for most use cases.
    Two,

    /// Dialect 3 (deprecated)
    ///
    /// https://redis.io/docs/latest/develop/ai/search-and-query/advanced-concepts/dialects/#dialect-3-deprecated
    #[deprecated(
        since = "1.0.0",
        note = "Dialect 3 is deprecated. Use QueryDialect::Two unless you know exactly what you are doing."
    )]
    Three,

    /// Dialect 4 (deprecated)
    ///
    /// https://redis.io/docs/latest/develop/ai/search-and-query/advanced-concepts/dialects/#dialect-4-deprecated
    #[deprecated(
        since = "1.0.0",
        note = "Dialect 4 is deprecated. Use QueryDialect::Two unless you know exactly what you are doing."
    )]
    Four,
}

impl Default for QueryDialect {
    fn default() -> Self {
        QueryDialect::Two
    }
}

#[allow(deprecated)]
impl ToRedisArgs for QueryDialect {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        let value = match self {
            QueryDialect::One => 1,
            QueryDialect::Two => 2,
            QueryDialect::Three => 3,
            QueryDialect::Four => 4,
        };
        value.write_redis_args(out);
    }
}

/// Numeric filter for FILTER option
/// This filter uses the same syntax as [ZRANGE](https://redis.io/docs/latest/commands/zrange/)
#[derive(Clone, Debug)]
pub struct NumericFilter {
    numeric_attribute: String,
    min: Bound<f64>,
    max: Bound<f64>,
}

impl NumericFilter {
    /// Create a new numeric filter
    pub fn new<S: Into<String>>(numeric_attribute: S, min: Bound<f64>, max: Bound<f64>) -> Self {
        Self {
            numeric_attribute: numeric_attribute.into(),
            min,
            max,
        }
    }
}

impl ToRedisArgs for NumericFilter {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        out.write_arg(b"FILTER");
        self.numeric_attribute.write_redis_args(out);
        match self.min {
            Bound::Excluded(min) => {
                // write "(" prefix for exclusive bound as a single argument with the value
                // Use writer_for_next_arg to write both "(" and the number as one argument
                use std::io::Write;
                let mut writer = out.writer_for_next_arg();
                writer.write_all(b"(").unwrap();
                // Use ryu to format the float, which preserves decimals
                let mut buf = ryu::Buffer::new();
                writer.write_all(buf.format(min).as_bytes()).unwrap();
            }
            Bound::Included(min) => {
                min.write_redis_args(out);
            }
            Bound::Unbounded => out.write_arg(b"-inf"),
        }
        match self.max {
            Bound::Excluded(max) => {
                // write "(" prefix for exclusive bound as a single argument with the value
                use std::io::Write;
                let mut writer = out.writer_for_next_arg();
                writer.write_all(b"(").unwrap();
                // Use ryu to format the float, which preserves decimals
                let mut buf = ryu::Buffer::new();
                writer.write_all(buf.format(max).as_bytes()).unwrap();
            }
            Bound::Included(max) => {
                max.write_redis_args(out);
            }
            Bound::Unbounded => out.write_arg(b"+inf"),
        }
    }
}

/// Geographic filter for GEOFILTER option
pub struct GeoFilter {
    field: String,
    lon: f64,
    lat: f64,
    radius: f64,
    unit: Unit,
}

impl GeoFilter {
    /// Create a new geographic filter
    pub fn new<S: Into<String>>(field: S, lon: f64, lat: f64, radius: f64, unit: Unit) -> Self {
        Self {
            field: field.into(),
            lon,
            lat,
            radius,
            unit,
        }
    }
}

impl ToRedisArgs for GeoFilter {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        out.write_arg(b"GEOFILTER");
        self.field.write_redis_args(out);
        self.lon.write_redis_args(out);
        self.lat.write_redis_args(out);
        self.radius.write_redis_args(out);
        self.unit.write_redis_args(out);
    }
}

/// Return field specification
#[derive(Clone, Debug)]
pub struct ReturnField {
    identifier: String,
    alias: Option<String>,
}

impl ReturnField {
    /// Create a new return field
    pub fn new<S: Into<String>>(identifier: S) -> Self {
        Self {
            identifier: identifier.into(),
            alias: None,
        }
    }

    /// Set an alias for the field
    pub fn alias<S: Into<String>>(mut self, alias: S) -> Self {
        self.alias = Some(alias.into());
        self
    }
}

impl ToRedisArgs for ReturnField {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        self.identifier.write_redis_args(out);
        if let Some(ref alias) = self.alias {
            out.write_arg(b"AS");
            alias.write_redis_args(out);
        }
    }

    fn num_of_args(&self) -> usize {
        if self.alias.is_some() { 3 } else { 1 }
    }
}

/// Summarize options for SUMMARIZE
#[derive(Clone, Debug, Default)]
pub struct SummarizeOptions {
    fields: Vec<String>,
    frags: Option<u32>,
    len: Option<u32>,
    separator: Option<String>,
}

impl SummarizeOptions {
    /// Create new summarize options
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a field to summarize
    pub fn field<S: Into<String>>(mut self, field: S) -> Self {
        self.fields.push(field.into());
        self
    }

    /// Add multiple fields to summarize
    ///
    /// # Example
    ///
    /// ```rust
    /// use redis::search::SummarizeOptions;
    ///
    /// let options = SummarizeOptions::new()
    ///     .fields(["title", "description", "content"])
    ///     .frags(3)
    ///     .len(20);
    /// ```
    pub fn fields<I, S>(mut self, fields: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.fields.extend(fields.into_iter().map(|f| f.into()));
        self
    }

    /// Set the number of fragments
    pub fn frags(mut self, frags: u32) -> Self {
        self.frags = Some(frags);
        self
    }

    /// Set the fragment length
    pub fn len(mut self, len: u32) -> Self {
        self.len = Some(len);
        self
    }

    /// Set the separator
    pub fn separator<S: Into<String>>(mut self, separator: S) -> Self {
        self.separator = Some(separator.into());
        self
    }
}

impl ToRedisArgs for SummarizeOptions {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        if !self.fields.is_empty() {
            out.write_arg(b"FIELDS");
            self.fields.len().write_redis_args(out);
            for field in &self.fields {
                field.write_redis_args(out);
            }
        }

        if let Some(frags) = self.frags {
            out.write_arg(b"FRAGS");
            frags.write_redis_args(out);
        }

        if let Some(len) = self.len {
            out.write_arg(b"LEN");
            len.write_redis_args(out);
        }

        if let Some(ref separator) = self.separator {
            out.write_arg(b"SEPARATOR");
            separator.write_redis_args(out);
        }
    }
}

/// Highlight options for HIGHLIGHT
#[derive(Clone, Debug, Default)]
pub struct HighlightOptions {
    fields: Vec<String>,
    open_tag: Option<String>,
    close_tag: Option<String>,
}

impl HighlightOptions {
    /// Create new highlight options
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a field to highlight
    pub fn field<S: Into<String>>(mut self, field: S) -> Self {
        self.fields.push(field.into());
        self
    }

    /// Set the opening and closing tags
    pub fn tags<S: Into<String>>(mut self, open: S, close: S) -> Self {
        self.open_tag = Some(open.into());
        self.close_tag = Some(close.into());
        self
    }
}

impl ToRedisArgs for HighlightOptions {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        if !self.fields.is_empty() {
            out.write_arg(b"FIELDS");
            self.fields.len().write_redis_args(out);
            for field in &self.fields {
                field.write_redis_args(out);
            }
        }

        if let Some(ref open) = self.open_tag {
            if let Some(ref close) = self.close_tag {
                out.write_arg(b"TAGS");
                open.write_redis_args(out);
                close.write_redis_args(out);
            }
        }
    }
}

/// Scoring function for FT.SEARCH
///
/// Scoring functions evaluate document relevance based on document scores and term frequency.
/// See [Redis documentation](https://redis.io/docs/latest/develop/ai/search-and-query/advanced-concepts/scoring/) for details.
#[derive(Clone, Copy, Debug)]
#[non_exhaustive]
pub enum ScoringFunction {
    /// Basic TF-IDF scoring with frequency normalization and distance penalties
    ///
    /// For each term, calculates TF-IDF score weighted by field weights.
    /// Applies penalties based on distance between search terms (slop).
    /// Exact matches get no penalty, distant terms have reduced scores.
    Tfidf,

    /// TF-IDF with document length normalization
    ///
    /// Identical to TFIDF, but term frequencies are normalized by document length
    TfidfDocnorm,

    /// BM25 scoring algorithm (default)
    ///
    /// A variation on basic TF-IDF.
    Bm25Std,

    /// BM25 with min-max normalization
    ///
    /// Uses min-max normalization across the collection for better accuracy
    /// when term frequency distributions vary significantly. More accurate but
    /// slower due to global statistics computation.
    Bm25StdNorm,

    /// BM25 with tanh normalization
    ///
    /// Applies smooth transformation using tanh(x/factor) function.
    /// Faster and more scalable than min-max normalization, but less accurate.
    /// Can optionally specify a smoothing factor (default: 4).
    Bm25StdTanh {
        /// Smoothing factor for the tanh function (default: 4)
        factor: Option<u32>,
    },

    /// DISMAX scoring
    ///
    /// Simple scorer that sums frequencies of matched terms.
    /// For union clauses, gives the maximum value of those matches.
    /// No other penalties or factors applied.
    Dismax,

    /// Document score only
    ///
    /// Returns only the presumptive document score without any calculations.
    /// Useful when using external scores that can be updated.
    Docscore,

    /// Hamming distance scoring
    ///
    /// Scores by inverse Hamming distance between document and query payloads.
    /// Uses 1/(1+d) so distance 0 gives perfect score of 1.
    /// Requires both document and query to have payloads of equal length.
    /// Payloads with length multiple of 64 bits are slightly faster.
    Hamming,
}

impl ToRedisArgs for ScoringFunction {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        match self {
            ScoringFunction::Tfidf => out.write_arg(b"TFIDF"),
            ScoringFunction::TfidfDocnorm => out.write_arg(b"TFIDF.DOCNORM"),
            ScoringFunction::Bm25Std => out.write_arg(b"BM25STD"),
            ScoringFunction::Bm25StdNorm => out.write_arg(b"BM25STD.NORM"),
            ScoringFunction::Bm25StdTanh { factor } => {
                out.write_arg(b"BM25STD.TANH");
                if let Some(f) = factor {
                    out.write_arg(b"BM25STD_TANH_FACTOR");
                    f.write_redis_args(out);
                }
            }
            ScoringFunction::Dismax => out.write_arg(b"DISMAX"),
            ScoringFunction::Docscore => out.write_arg(b"DOCSCORE"),
            ScoringFunction::Hamming => out.write_arg(b"HAMMING"),
        }
    }
}

/// Sort direction for search results
#[derive(Clone, Copy, Debug)]
pub enum SortDirection {
    /// Ascending direction
    Asc,
    /// Descending direction
    Desc,
}

impl ToRedisArgs for SortDirection {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        out.write_arg(match self {
            SortDirection::Asc => b"ASC",
            SortDirection::Desc => b"DESC",
        });
    }
}

/// Sort by field
/// TODO: add support for the WITHCOUNT parameter
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum SortBy {
    /// Sort by field
    Field(String),
    /// Sort by field with direction
    FieldWithDirection(String, SortDirection),
}

impl ToRedisArgs for SortBy {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        match self {
            SortBy::Field(field) => field.write_redis_args(out),
            SortBy::FieldWithDirection(field, direction) => {
                field.write_redis_args(out);
                direction.write_redis_args(out);
            }
        }
    }
}

/// Limits the results to the offset (zero-indexed) and number of results given.
/// LIMIT 0 0 can be used to count the number of documents in the result set without actually returning them.
/// LIMIT behavior: When using the LIMIT option without sorting, the results returned are non-deterministic, which means that subsequent queries may return duplicated or missing values.
#[derive(Clone, Copy, Debug)]
pub struct Limit {
    first: usize, // Zero-indexed offset
    num: usize,
}

impl ToRedisArgs for Limit {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        self.first.write_redis_args(out);
        self.num.write_redis_args(out);
    }
}

impl From<(usize, usize)> for Limit {
    fn from((first, num): (usize, usize)) -> Self {
        Self { first, num }
    }
}

/// Query parameter for PARAMS
#[derive(Clone, Debug)]
pub struct QueryParam {
    name: String,
    value: String,
}

impl QueryParam {
    /// Create a new query parameter
    pub fn new<S: Into<String>>(name: S, value: S) -> Self {
        Self {
            name: name.into(),
            value: value.into(),
        }
    }
}

impl<N, V> From<(N, V)> for QueryParam
where
    N: Into<String>,
    V: Into<String>,
{
    fn from((name, value): (N, V)) -> Self {
        Self {
            name: name.into(),
            value: value.into(),
        }
    }
}

impl ToRedisArgs for QueryParam {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        self.name.write_redis_args(out);
        self.value.write_redis_args(out);
    }
}

/// Optional arguments for the FT.SEARCH command
#[must_use = "Options have no effect unless passed to a command"]
#[non_exhaustive]
pub struct SearchOptions {
    nocontent: bool,
    verbatim: bool,
    // nostopwords: bool, // Deprecated
    withscores: bool,
    // withpayloads: bool, // TODO: Enable once FT.ADD is added
    withsortkeys: bool,
    filters: Vec<NumericFilter>,
    geofilters: Vec<GeoFilter>,
    inkeys: Vec<String>,
    infields: Vec<String>,
    return_fields: Vec<ReturnField>,
    summarize: Option<SummarizeOptions>,
    highlight: Option<HighlightOptions>,
    slop: Option<u32>,
    timeout: Option<u32>,
    inorder: bool,
    language: Option<SearchLanguage>,
    expander: Option<String>,
    scorer: Option<String>,
    scoring_function: Option<ScoringFunction>,
    explainscore: bool,
    payload: Option<String>,
    sortby: Option<SortBy>,
    limit: Option<Limit>,
    params: Vec<QueryParam>,
    dialect: QueryDialect,
}

impl Default for SearchOptions {
    fn default() -> Self {
        Self {
            nocontent: false,
            verbatim: false,
            withscores: false,
            withsortkeys: false,
            filters: Vec::new(),
            geofilters: Vec::new(),
            inkeys: Vec::new(),
            infields: Vec::new(),
            return_fields: Vec::new(),
            summarize: None,
            highlight: None,
            slop: None,
            timeout: None,
            inorder: false,
            language: None,
            expander: None,
            scorer: None,
            scoring_function: None,
            explainscore: false,
            payload: None,
            sortby: None,
            limit: None,
            params: Vec::new(),
            dialect: QueryDialect::default(),
        }
    }
}

impl SearchOptions {
    /// Create a new SearchOptions
    pub fn new() -> Self {
        Self::default()
    }

    /// Do not return document contents, only IDs
    pub fn nocontent(mut self) -> Self {
        self.nocontent = true;
        self
    }

    /// Do not use stemming for query expansion
    pub fn verbatim(mut self) -> Self {
        self.verbatim = true;
        self
    }

    /// Return the document scores
    pub fn withscores(mut self) -> Self {
        self.withscores = true;
        self
    }

    // /// Return document payloads (requires FT.ADD)
    // pub fn withpayloads(mut self) -> Self {
    //     self.withpayloads = true;
    //     self
    // }

    /// Return the sort keys
    pub fn withsortkeys(mut self) -> Self {
        self.withsortkeys = true;
        self
    }

    /// Add a numeric filter
    pub fn filter(mut self, filter: NumericFilter) -> Self {
        self.filters.push(filter);
        self
    }

    /// Add a geographic filter
    pub fn geofilter(mut self, geofilter: GeoFilter) -> Self {
        self.geofilters.push(geofilter);
        self
    }

    /// Limit the search to specific keys
    pub fn inkey<S: Into<String>>(mut self, key: S) -> Self {
        self.inkeys.push(key.into());
        self
    }

    /// Limit the search to multiple specific keys
    ///
    /// # Example
    ///
    /// ```rust
    /// use redis::search::SearchOptions;
    ///
    /// let options = SearchOptions::new()
    ///     .inkeys(["product:1", "product:2", "product:3"]);
    /// ```
    pub fn inkeys<I, S>(mut self, keys: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.inkeys.extend(keys.into_iter().map(|k| k.into()));
        self
    }

    /// Limit the search to specific fields
    pub fn infield<S: Into<String>>(mut self, field: S) -> Self {
        self.infields.push(field.into());
        self
    }

    /// Limit the search to multiple specific fields
    ///
    /// # Example
    ///
    /// ```rust
    /// use redis::search::SearchOptions;
    ///
    /// let options = SearchOptions::new()
    ///     .infields(["title", "description", "category"]);
    /// ```
    pub fn infields<I, S>(mut self, fields: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.infields.extend(fields.into_iter().map(|f| f.into()));
        self
    }

    /// Add a field to return
    pub fn return_field(mut self, field: ReturnField) -> Self {
        self.return_fields.push(field);
        self
    }

    /// Add multiple fields to return
    ///
    /// # Example
    ///
    /// ```rust
    /// use redis::search::{SearchOptions, ReturnField};
    ///
    /// let options = SearchOptions::new()
    ///     .return_fields([
    ///         ReturnField::new("title"),
    ///         ReturnField::new("price").alias("cost"),
    ///         ReturnField::new("description"),
    ///     ]);
    /// ```
    pub fn return_fields<I>(mut self, fields: I) -> Self
    where
        I: IntoIterator<Item = ReturnField>,
    {
        self.return_fields.extend(fields);
        self
    }

    /// Set summarize options
    pub fn summarize(mut self, summarize: SummarizeOptions) -> Self {
        self.summarize = Some(summarize);
        self
    }

    /// Set highlight options
    pub fn highlight(mut self, highlight: HighlightOptions) -> Self {
        self.highlight = Some(highlight);
        self
    }

    /// Set the slop value for phrase queries
    pub fn slop(mut self, slop: u32) -> Self {
        self.slop = Some(slop);
        self
    }

    /// Set the query timeout in milliseconds
    pub fn timeout(mut self, timeout: u32) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Require terms to appear in order
    pub fn inorder(mut self) -> Self {
        self.inorder = true;
        self
    }

    /// Set the query language
    pub fn language(mut self, language: SearchLanguage) -> Self {
        self.language = Some(language);
        self
    }

    /// Set the query expander
    pub fn expander<S: Into<String>>(mut self, expander: S) -> Self {
        self.expander = Some(expander.into());
        self
    }

    /// Set the scoring function using a custom string
    ///
    /// For standard scoring functions, prefer using [`scoring_function`](Self::scoring_function)
    /// which provides type-safe options.
    ///
    /// # Example
    /// ```rust
    /// use redis::search::SearchOptions;
    ///
    /// let options = SearchOptions::new()
    ///     .scorer("my_custom_scorer");
    /// ```
    pub fn scorer<S: Into<String>>(mut self, scorer: S) -> Self {
        self.scorer = Some(scorer.into());
        self
    }

    /// Set the scoring function using a type-safe enum
    ///
    /// # Example
    /// ```rust
    /// use redis::search::{SearchOptions, ScoringFunction};
    ///
    /// let options = SearchOptions::new()
    ///     .scoring_function(ScoringFunction::Bm25Std);
    ///
    /// // With BM25STD.TANH and custom factor
    /// let options_tanh = SearchOptions::new()
    ///     .scoring_function(ScoringFunction::Bm25StdTanh { factor: Some(12) });
    /// ```
    pub fn scoring_function(mut self, scoring_function: ScoringFunction) -> Self {
        self.scoring_function = Some(scoring_function);
        self
    }

    /// Returns a textual description of how the scores were calculated.
    /// Using this option requires WITHSCORES.
    pub fn explainscore(mut self) -> Self {
        // Automatically set withscores to prevent the server from returning an error
        self.withscores = true;
        self.explainscore = true;
        self
    }

    /// Set a payload
    pub fn payload<S: Into<String>>(mut self, payload: S) -> Self {
        self.payload = Some(payload.into());
        self
    }

    /// Sort results by a field with optional direction
    pub fn sortby<S: Into<String>>(mut self, field: S, direction: Option<SortDirection>) -> Self {
        self.sortby = Some(match direction {
            Some(direction) => SortBy::FieldWithDirection(field.into(), direction),
            None => SortBy::Field(field.into()),
        });
        self
    }

    /// Set the limit
    pub fn limit<L: Into<Limit>>(mut self, limit: L) -> Self {
        self.limit = Some(limit.into());
        self
    }

    /// Add a query parameter
    ///
    /// # Example
    ///
    /// ```rust
    /// use redis::search::{SearchOptions, QueryParam};
    ///
    /// // Using QueryParam::new
    /// let options = SearchOptions::new()
    ///     .param(QueryParam::new("term", "laptop"));
    ///
    /// // Using a tuple (more concise)
    /// let options = SearchOptions::new()
    ///     .param(("term", "laptop"));
    /// ```
    pub fn param<P: Into<QueryParam>>(mut self, param: P) -> Self {
        self.params.push(param.into());
        self
    }

    /// Add multiple query parameters
    ///
    /// # Example
    ///
    /// ```rust
    /// use redis::search::{SearchOptions, QueryParam};
    ///
    /// // Using QueryParam::new
    /// let options = SearchOptions::new()
    ///     .params([
    ///         QueryParam::new("term", "laptop"),
    ///         QueryParam::new("min", "100"),
    ///         QueryParam::new("max", "500"),
    ///     ]);
    ///
    /// // Using tuples
    /// let options = SearchOptions::new()
    ///     .params([
    ///         ("term", "laptop"),
    ///         ("min", "100"),
    ///         ("max", "500"),
    ///     ]);
    /// ```
    pub fn params<I, P>(mut self, params: I) -> Self
    where
        I: IntoIterator<Item = P>,
        P: Into<QueryParam>,
    {
        self.params.extend(params.into_iter().map(|p| p.into()));
        self
    }

    /// Set the query dialect version
    ///
    /// # Example
    /// ```rust
    /// use redis::search::*;
    ///
    /// let options = SearchOptions::new()
    ///     .dialect(QueryDialect::Two);
    /// ```
    pub fn dialect(mut self, dialect: QueryDialect) -> Self {
        self.dialect = dialect;
        self
    }
}

impl ToRedisArgs for SearchOptions {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        if self.nocontent {
            out.write_arg(b"NOCONTENT");
        }

        if self.verbatim {
            out.write_arg(b"VERBATIM");
        }

        if self.withscores {
            out.write_arg(b"WITHSCORES");
        }

        // if self.withpayloads {
        //     out.write_arg(b"WITHPAYLOADS");
        // }

        if self.withsortkeys {
            out.write_arg(b"WITHSORTKEYS");
        }

        for filter in &self.filters {
            filter.write_redis_args(out);
        }

        for geofilter in &self.geofilters {
            geofilter.write_redis_args(out);
        }

        if !self.inkeys.is_empty() {
            out.write_arg(b"INKEYS");
            self.inkeys.len().write_redis_args(out);
            for key in &self.inkeys {
                key.write_redis_args(out);
            }
        }

        if !self.infields.is_empty() {
            out.write_arg(b"INFIELDS");
            self.infields.len().write_redis_args(out);
            for field in &self.infields {
                field.write_redis_args(out);
            }
        }

        if !self.return_fields.is_empty() {
            out.write_arg(b"RETURN");
            // Count total arguments: each field is 1 arg, or 3 args if it has an alias
            let total_args: usize = self.return_fields.iter().map(|f| f.num_of_args()).sum();
            total_args.write_redis_args(out);
            for field in &self.return_fields {
                field.write_redis_args(out);
            }
        }

        if let Some(ref summarize) = self.summarize {
            out.write_arg(b"SUMMARIZE");
            summarize.write_redis_args(out);
        }

        if let Some(ref highlight) = self.highlight {
            out.write_arg(b"HIGHLIGHT");
            highlight.write_redis_args(out);
        }

        if let Some(slop) = self.slop {
            out.write_arg(b"SLOP");
            slop.write_redis_args(out);
        }

        if let Some(timeout) = self.timeout {
            out.write_arg(b"TIMEOUT");
            timeout.write_redis_args(out);
        }

        if self.inorder {
            out.write_arg(b"INORDER");
        }

        if let Some(ref language) = self.language {
            out.write_arg(b"LANGUAGE");
            language.write_redis_args(out);
        }

        if let Some(ref expander) = self.expander {
            out.write_arg(b"EXPANDER");
            expander.write_redis_args(out);
        }

        // Handle scoring function (prefer enum over string)
        if let Some(ref scoring_function) = self.scoring_function {
            out.write_arg(b"SCORER");
            scoring_function.write_redis_args(out);
        } else if let Some(ref scorer) = self.scorer {
            out.write_arg(b"SCORER");
            scorer.write_redis_args(out);
        }

        if self.explainscore {
            out.write_arg(b"EXPLAINSCORE");
        }

        if let Some(ref payload) = self.payload {
            out.write_arg(b"PAYLOAD");
            payload.write_redis_args(out);
        }

        if let Some(ref sortby) = self.sortby {
            out.write_arg(b"SORTBY");
            sortby.write_redis_args(out);
        }

        if let Some(ref limit) = self.limit {
            out.write_arg(b"LIMIT");
            limit.write_redis_args(out);
        }

        if !self.params.is_empty() {
            out.write_arg(b"PARAMS");
            (self.params.len() * 2).write_redis_args(out);
            for param in &self.params {
                param.write_redis_args(out);
            }
        }

        out.write_arg(b"DIALECT");
        self.dialect.write_redis_args(out);
    }
}
