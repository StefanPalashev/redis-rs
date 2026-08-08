//! Defines the vector field types used with the FT.CREATE command.
//!
//! A vector field is written as `VECTOR <algorithm> <attribute_count> <attributes...>`, where the
//! count covers the shared attributes (`TYPE`, `DIM`, `DISTANCE_METRIC`) plus whichever
//! algorithm-specific ones were set. Each algorithm's options and builder therefore live in their
//! own module and report their argument count through `ToRedisArgs::num_of_args`, which
//! [`VectorField`] sums to produce the count written on the wire.
use super::fields::{BaseSchemaField, FieldType};
use crate::{RedisWrite, ToRedisArgs};

mod flat;
mod hnsw;
mod vamana;

pub use flat::*;
pub use hnsw::*;
pub use vamana::*;

#[derive(Debug, Clone)]
#[non_exhaustive]
pub(crate) enum VectorAlgorithm {
    Flat,
    Hnsw,
    Vamana,
}

impl ToRedisArgs for VectorAlgorithm {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        out.write_arg(match self {
            VectorAlgorithm::Flat => b"FLAT",
            VectorAlgorithm::Hnsw => b"HNSW",
            VectorAlgorithm::Vamana => b"SVS-VAMANA",
        });
    }
}

/// Vector type for vector fields
#[derive(Debug, Copy, Clone)]
#[non_exhaustive]
#[allow(missing_docs)]
pub enum VectorType {
    Float32,
    Float64,
    BFloat16,
    Float16,
    Int8,
    UInt8,
}

impl ToRedisArgs for VectorType {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        out.write_arg(match self {
            VectorType::Float32 => b"FLOAT32",
            VectorType::Float64 => b"FLOAT64",
            VectorType::BFloat16 => b"BFLOAT16",
            VectorType::Float16 => b"FLOAT16",
            VectorType::Int8 => b"INT8",
            VectorType::UInt8 => b"UINT8",
        });
    }
}

/// [Distance metric](https://redis.io/docs/latest/develop/ai/search-and-query/vectors/#distance-metrics/) for vector fields
#[derive(Debug, Copy, Clone)]
#[non_exhaustive]
pub enum DistanceMetric {
    /// Euclidean distance between two vectors.
    L2,
    /// Inner product of two vectors.
    IP,
    /// Cosine distance of two vectors.
    Cosine,
}

impl ToRedisArgs for DistanceMetric {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        out.write_arg(match self {
            DistanceMetric::L2 => b"L2",
            DistanceMetric::IP => b"IP",
            DistanceMetric::Cosine => b"COSINE",
        });
    }
}

/// Represents a vector field in the schema.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct SchemaVectorField {
    pub(crate) base: BaseSchemaField,
    algorithm: VectorAlgorithm,
    vector_type: VectorType,
    dim: u32,
    distance_metric: DistanceMetric,
}

impl ToRedisArgs for SchemaVectorField {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        if let Some(alias) = &self.base.alias {
            out.write_arg(b"AS");
            alias.write_redis_args(out);
        }

        self.base.field_type.write_redis_args(out);

        self.algorithm.write_redis_args(out);
        // Note: The attribute count will be written by the VectorField implementation
        // which knows about both base and algorithm-specific attributes
        // That is:
        /*
            out.write_arg(b"TYPE");
            self.vector_type.write_redis_args(out);
            out.write_arg(b"DIM");
            self.dim.write_redis_args(out);
            out.write_arg(b"DISTANCE_METRIC");
            self.distance_metric.write_redis_args(out);
        */
    }

    fn num_of_args(&self) -> usize {
        // Count the number of attribute pairs (key-value pairs) for this vector field.
        // Base attributes are: TYPE, DIM, DISTANCE_METRIC (3 pairs = 6 args)
        6
    }
}

/// Represents a vector field in the schema, built through a per-algorithm builder.
///
/// # Algorithms
///
/// - **FLAT**: Brute-force exact search. Best for small datasets (< 1M vectors) where perfect accuracy is required.
/// - **HNSW**: Hierarchical Navigable Small World graph-based approximate search. Best for large datasets (> 1M vectors)
///   where search performance and scalability are more important than perfect accuracy.
/// - **SVS-VAMANA**: Intel's Scalable Vector Search with graph-based approximate search and compression support.
///   Best when you need high performance with reduced memory usage, especially on Intel hardware.
///   More information at: <https://intel.github.io/ScalableVectorSearch/intro.html>
///
/// # Examples
///
/// ```rust
/// use redis::search::*;
///
/// // FLAT index for exact search
/// let flat_field = VectorField::flat(VectorType::Float32, 128, DistanceMetric::Cosine)
///     .block_size(1000)
///     .build();
///
/// // HNSW index for approximate search
/// let hnsw_field = VectorField::hnsw(VectorType::Float32, 128, DistanceMetric::Cosine)
///     .m(16)
///     .ef_construction(200)
///     .build();
///
/// // VAMANA index with compression (note: uses VamanaVectorType for type safety)
/// let vamana_field = VectorField::vamana(VamanaVectorType::Float32, 128, DistanceMetric::Cosine)
///     .compression(CompressionType::LVQ8)
///     .graph_max_degree(64)
///     .build();
/// ```
#[must_use = "Vector field has no effect unless inserted into a schema"]
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum VectorField {
    /// FLAT (brute-force) vector index for exact nearest neighbor search.
    /// Best for small datasets (< 1M vectors) where perfect accuracy is required.
    Flat(SchemaVectorField, FlatVectorOptions),

    /// HNSW (Hierarchical Navigable Small World) vector index for approximate nearest neighbor search.
    /// Best for large datasets (> 1M vectors) where performance is more important than perfect accuracy.
    Hnsw(SchemaVectorField, HnswVectorOptions),

    /// SVS-VAMANA vector index with compression support for memory-efficient approximate search.
    /// Best when you need high performance with reduced memory usage, especially on Intel hardware.
    Vamana(SchemaVectorField, VamanaVectorOptions),
}

impl VectorField {
    /// Set the alias for the field.
    pub fn alias(mut self, alias: impl Into<String>) -> Self {
        match self {
            VectorField::Flat(ref mut base, _)
            | VectorField::Hnsw(ref mut base, _)
            | VectorField::Vamana(ref mut base, _) => base.base = base.base.clone().alias(alias),
        };
        self
    }

    /// Set index missing. This allows searching for missing values - documents that do not contain a specific field.
    pub fn index_missing(mut self, index_missing: bool) -> Self {
        match self {
            VectorField::Flat(ref mut base, _)
            | VectorField::Hnsw(ref mut base, _)
            | VectorField::Vamana(ref mut base, _) => {
                base.base = base.base.clone().index_missing(index_missing)
            }
        };
        self
    }
}

impl ToRedisArgs for VectorField {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        let base = match self {
            VectorField::Flat(base, _)
            | VectorField::Hnsw(base, _)
            | VectorField::Vamana(base, _) => base,
        };
        base.write_redis_args(out);

        let attributes_count = match self {
            VectorField::Flat(base, flat_vector_options) => {
                base.num_of_args() + flat_vector_options.num_of_args()
            }
            VectorField::Hnsw(base, hnsw_vector_options) => {
                base.num_of_args() + hnsw_vector_options.num_of_args()
            }
            VectorField::Vamana(base, vamana_vector_options) => {
                base.num_of_args() + vamana_vector_options.num_of_args()
            }
        };
        attributes_count.write_redis_args(out);

        out.write_arg(b"TYPE");
        base.vector_type.write_redis_args(out);
        out.write_arg(b"DIM");
        base.dim.write_redis_args(out);
        out.write_arg(b"DISTANCE_METRIC");
        base.distance_metric.write_redis_args(out);

        // Write algorithm-specific attributes
        match self {
            VectorField::Flat(_, flat_vector_options) => {
                flat_vector_options.write_redis_args(out);
            }
            VectorField::Hnsw(_, hnsw_vector_options) => {
                hnsw_vector_options.write_redis_args(out);
            }
            VectorField::Vamana(_, vamana_vector_options) => {
                vamana_vector_options.write_redis_args(out);
            }
        }

        if base.base.index_missing {
            out.write_arg(b"INDEXMISSING");
        }
    }
}

impl VectorField {
    /// Create a new FLAT vector field
    pub fn flat(
        vector_type: VectorType,
        dim: u32,
        distance_metric: DistanceMetric,
    ) -> FlatVectorFieldBuilder {
        assert!(
            dim > 0,
            "Vector dimension must be positive (greater than 0)"
        );

        FlatVectorFieldBuilder::new(SchemaVectorField {
            base: BaseSchemaField::new(FieldType::Vector),
            algorithm: VectorAlgorithm::Flat,
            vector_type,
            dim,
            distance_metric,
        })
    }

    /// Create a new HNSW vector field
    pub fn hnsw(
        vector_type: VectorType,
        dim: u32,
        distance_metric: DistanceMetric,
    ) -> HnswVectorFieldBuilder {
        assert!(
            dim > 0,
            "Vector dimension must be positive (greater than 0)"
        );

        HnswVectorFieldBuilder::new(SchemaVectorField {
            base: BaseSchemaField::new(FieldType::Vector),
            algorithm: VectorAlgorithm::Hnsw,
            vector_type,
            dim,
            distance_metric,
        })
    }

    /// Create a new VAMANA vector field
    pub fn vamana(
        vector_type: VamanaVectorType,
        dim: u32,
        distance_metric: DistanceMetric,
    ) -> VamanaVectorFieldBuilder {
        assert!(
            dim > 0,
            "Vector dimension must be positive (greater than 0)"
        );

        VamanaVectorFieldBuilder::new(SchemaVectorField {
            base: BaseSchemaField::new(FieldType::Vector),
            algorithm: VectorAlgorithm::Vamana,
            vector_type: vector_type.into(),
            dim,
            distance_metric,
        })
    }
}
