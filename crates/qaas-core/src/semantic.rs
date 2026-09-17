//! Embedding-similarity primitives: near-duplicate detection and routing
//! by cosine similarity, generic over what's being indexed.
//!
//! This crate has no embedding model of its own, and deliberately never
//! will — nothing else in this codebase calls out to an LLM or embedding
//! provider (agents do that; this is their queue, not their inference
//! backend), and bolting one on here would mean an HTTP client, API key
//! configuration, and real inference cost inside what's supposed to be
//! infrastructure, not a client. [`Embedding`] is therefore a *validated
//! container* for a vector a caller already computed, not something this
//! module produces. `qaas-server`'s MCP layer
//! (`feature/semantic-dedup-routing`) is where a caller's embedding
//! actually gets used: an `enqueue` call carrying one gets checked
//! against other embeddings recently seen on the same queue (dedup), or
//! against every registered queue's descriptor embedding (routing) — see
//! that module's own docs for the two concrete uses of the generic
//! [`EmbeddingIndex`] built here.
//!
//! Same "generic primitive, integrated at the layer that actually
//! understands the domain" split this crate already applies to
//! [`circuit_breaker`](crate::circuit_breaker) and
//! [`admission`](crate::admission): nothing here knows about
//! `MessageId`, queue names, or the MCP protocol.

use std::collections::HashMap;
use std::hash::Hash;
use std::time::{Duration, Instant};

use tokio::sync::Mutex;

/// A non-empty vector of finite floats, validated once at construction so
/// every other operation in this module can assume it never has to
/// re-check for `NaN`, infinities, or a zero-length vector.
#[derive(Debug, Clone, PartialEq)]
pub struct Embedding(Vec<f32>);

/// `values` passed to [`Embedding::new`] was empty, or contained a
/// non-finite (`NaN` or infinite) component.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("embedding must be non-empty and contain only finite values")]
pub struct InvalidEmbedding;

impl Embedding {
    /// Validates and wraps `values`.
    ///
    /// # Errors
    ///
    /// Returns [`InvalidEmbedding`] if `values` is empty or contains a
    /// `NaN` or infinite component — garbage a caller passed through
    /// unchecked would otherwise silently corrupt every similarity score
    /// computed against it, rather than being rejected up front where
    /// the caller can see why.
    pub fn new(values: Vec<f32>) -> Result<Self, InvalidEmbedding> {
        if values.is_empty() || values.iter().any(|value| !value.is_finite()) {
            return Err(InvalidEmbedding);
        }
        Ok(Self(values))
    }

    /// How many components this embedding has.
    #[must_use]
    pub fn dimension(&self) -> usize {
        self.0.len()
    }

    /// Cosine similarity with `other`, in `-1.0..=1.0` (typically
    /// `0.0..=1.0` for real embedding models, whose components are
    /// usually non-negative, but this doesn't assume that).
    ///
    /// Returns `None` — not an error, since neither side did anything
    /// wrong — if `self` and `other` have different dimensions (they
    /// came from different embedding models, or one is simply the wrong
    /// shape) or either has zero magnitude (cosine similarity is
    /// undefined against the zero vector). Both cases are treated by
    /// [`EmbeddingIndex::nearest`] as "not a match," which is the
    /// correct behavior for both: incomparable embeddings should never
    /// win a similarity search.
    #[must_use]
    pub fn cosine_similarity(&self, other: &Embedding) -> Option<f32> {
        if self.0.len() != other.0.len() {
            return None;
        }
        let dot: f32 = self.0.iter().zip(&other.0).map(|(a, b)| a * b).sum();
        let norm_self = self.0.iter().map(|v| v * v).sum::<f32>().sqrt();
        let norm_other = other.0.iter().map(|v| v * v).sum::<f32>().sqrt();
        if norm_self == 0.0 || norm_other == 0.0 {
            return None;
        }
        Some(dot / (norm_self * norm_other))
    }
}

/// One [`EmbeddingIndex`] entry: the embedding itself, plus when it was
/// inserted so a TTL'd index knows when to forget it.
struct Entry {
    embedding: Embedding,
    inserted_at: Instant,
}

/// A keyed collection of embeddings, searchable by nearest cosine
/// similarity — generic over the key type so it serves both this
/// branch's uses (`qaas-server` indexes by `MessageId` for dedup, by
/// queue name for routing) without duplicating the search logic twice.
///
/// Optionally TTL'd: an index with `ttl: None` (routing — a registered
/// route is deliberate, operator-configured state, not ephemeral) keeps
/// every entry until explicitly [`remove`](Self::remove)d; one with
/// `ttl: Some(d)` (dedup) forgets an entry `d` after it was inserted,
/// regardless of whether anything ever called `remove` on it. This is
/// the safety net for dedup specifically: a message's embedding is
/// inserted when it's enqueued, and `qaas-server` removes it again on
/// `ack` — but nothing here can observe a `nack` that dead-letters a
/// message, or a consumer that simply crashes, so without a TTL a
/// message's embedding could linger in the index forever after the
/// message itself is long gone, silently "deduplicating" real new work
/// against a task that will never actually run. A bounded TTL turns an
/// unbounded correctness gap into a bounded, documented one instead.
pub struct EmbeddingIndex<K> {
    ttl: Option<Duration>,
    entries: Mutex<HashMap<K, Entry>>,
}

impl<K: Eq + Hash + Clone> EmbeddingIndex<K> {
    /// Creates an empty index. See the type's own docs for what `ttl`
    /// does and which of this branch's two uses wants which.
    #[must_use]
    pub fn new(ttl: Option<Duration>) -> Self {
        Self { ttl, entries: Mutex::new(HashMap::new()) }
    }

    /// Adds or replaces `key`'s embedding, resetting its TTL clock.
    pub async fn insert(&self, key: K, embedding: Embedding) {
        let mut entries = self.entries.lock().await;
        entries.insert(key, Entry { embedding, inserted_at: Instant::now() });
    }

    /// Removes `key`, if present. A no-op otherwise — same "already gone
    /// is not an error" stance [`ConsumerGroup::ack`](crate::ConsumerGroup::ack)
    /// takes on a stale lease.
    pub async fn remove(&self, key: &K) {
        self.entries.lock().await.remove(key);
    }

    /// Drops every entry whose TTL (if any) has elapsed. Called at the
    /// start of every other method here, the same lazy-eviction
    /// discipline [`AdmissionController`](crate::admission::AdmissionController)
    /// and [`FallbackCache`](crate::circuit_breaker::FallbackCache)
    /// already use — an index nobody is querying doesn't need to tick.
    fn evict_expired(entries: &mut HashMap<K, Entry>, ttl: Option<Duration>, now: Instant) {
        if let Some(ttl) = ttl {
            entries.retain(|_, entry| now.duration_since(entry.inserted_at) < ttl);
        }
    }

    /// The entry with the highest [`cosine_similarity`](Embedding::cosine_similarity)
    /// to `embedding`, and its score — `None` if the index is empty (after
    /// evicting expired entries) or nothing in it is comparable to
    /// `embedding` at all (see `cosine_similarity`'s own docs on `None`).
    ///
    /// Ties are broken arbitrarily (whichever a `HashMap` happens to
    /// iterate to first) — real embedding similarity scores essentially
    /// never land on an exact tie, and nothing about dedup or routing
    /// needs a *specific* winner among truly-tied candidates, only *a*
    /// winner.
    pub async fn nearest(&self, embedding: &Embedding) -> Option<(K, f32)> {
        let mut entries = self.entries.lock().await;
        let now = Instant::now();
        Self::evict_expired(&mut entries, self.ttl, now);

        entries
            .iter()
            .filter_map(|(key, entry)| {
                embedding.cosine_similarity(&entry.embedding).map(|score| (key.clone(), score))
            })
            .max_by(|(_, a), (_, b)| a.total_cmp(b))
    }

    /// How many entries this index currently holds, after evicting
    /// expired ones.
    pub async fn len(&self) -> usize {
        let mut entries = self.entries.lock().await;
        Self::evict_expired(&mut entries, self.ttl, Instant::now());
        entries.len()
    }

    /// Whether this index currently holds no entries, after evicting
    /// expired ones.
    pub async fn is_empty(&self) -> bool {
        self.len().await == 0
    }

    /// Every key currently in this index, after evicting expired ones —
    /// for a caller that wants to list what's registered (`qaas-server`'s
    /// `configure_route`) rather than search it. No defined order.
    pub async fn keys(&self) -> Vec<K> {
        let mut entries = self.entries.lock().await;
        Self::evict_expired(&mut entries, self.ttl, Instant::now());
        entries.keys().cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{Embedding, EmbeddingIndex, InvalidEmbedding};

    fn embedding(values: &[f32]) -> Embedding {
        Embedding::new(values.to_vec()).unwrap()
    }

    #[test]
    fn rejects_an_empty_vector() {
        assert_eq!(Embedding::new(vec![]), Err(InvalidEmbedding));
    }

    #[test]
    fn rejects_non_finite_components() {
        assert_eq!(Embedding::new(vec![1.0, f32::NAN]), Err(InvalidEmbedding));
        assert_eq!(Embedding::new(vec![1.0, f32::INFINITY]), Err(InvalidEmbedding));
    }

    #[test]
    fn identical_vectors_have_similarity_one() {
        let a = embedding(&[1.0, 2.0, 3.0]);
        let b = embedding(&[1.0, 2.0, 3.0]);
        let similarity = a.cosine_similarity(&b).unwrap();
        assert!((similarity - 1.0).abs() < 1e-6, "{similarity}");
    }

    #[test]
    fn orthogonal_vectors_have_similarity_zero() {
        let a = embedding(&[1.0, 0.0]);
        let b = embedding(&[0.0, 1.0]);
        let similarity = a.cosine_similarity(&b).unwrap();
        assert!(similarity.abs() < 1e-6, "{similarity}");
    }

    #[test]
    fn opposite_vectors_have_similarity_negative_one() {
        let a = embedding(&[1.0, 0.0]);
        let b = embedding(&[-1.0, 0.0]);
        let similarity = a.cosine_similarity(&b).unwrap();
        assert!((similarity + 1.0).abs() < 1e-6, "{similarity}");
    }

    #[test]
    fn mismatched_dimensions_are_not_comparable() {
        let a = embedding(&[1.0, 2.0]);
        let b = embedding(&[1.0, 2.0, 3.0]);
        assert_eq!(a.cosine_similarity(&b), None);
    }

    #[test]
    fn a_zero_vector_is_not_comparable_to_anything() {
        let zero = embedding(&[0.0, 0.0]);
        let other = embedding(&[1.0, 1.0]);
        assert_eq!(zero.cosine_similarity(&other), None);
        assert_eq!(zero.cosine_similarity(&zero), None);
    }

    #[tokio::test]
    async fn nearest_finds_the_closest_entry_among_several() {
        let index: EmbeddingIndex<&str> = EmbeddingIndex::new(None);
        // None of these is the query itself — "closest" is nearest
        // without being identical, so this actually exercises ranking
        // rather than just finding an exact match.
        index.insert("far", embedding(&[0.0, 1.0])).await;
        index.insert("close", embedding(&[0.9, 0.1])).await;
        index.insert("closest", embedding(&[0.99, 0.01])).await;

        let query = embedding(&[1.0, 0.0]);
        let (key, similarity) = index.nearest(&query).await.unwrap();
        assert_eq!(key, "closest");
        assert!(similarity > 0.99, "{similarity}");
    }

    #[tokio::test]
    async fn nearest_on_an_empty_index_is_none() {
        let index: EmbeddingIndex<&str> = EmbeddingIndex::new(None);
        assert_eq!(index.nearest(&embedding(&[1.0])).await, None);
    }

    #[tokio::test]
    async fn remove_takes_an_entry_out_of_consideration() {
        let index: EmbeddingIndex<&str> = EmbeddingIndex::new(None);
        index.insert("only", embedding(&[1.0, 0.0])).await;
        assert!(index.nearest(&embedding(&[1.0, 0.0])).await.is_some());

        index.remove(&"only").await;
        assert!(index.nearest(&embedding(&[1.0, 0.0])).await.is_none());
        assert_eq!(index.len().await, 0);
    }

    #[tokio::test]
    async fn keys_lists_every_current_entry_and_nothing_expired() {
        let index: EmbeddingIndex<&str> = EmbeddingIndex::new(Some(Duration::from_millis(50)));
        index.insert("stays", embedding(&[1.0, 0.0])).await;

        tokio::time::sleep(Duration::from_millis(30)).await;
        index.insert("fresher", embedding(&[0.0, 1.0])).await;

        tokio::time::sleep(Duration::from_millis(40)).await;
        // "stays" is now 70ms old (past the 50ms TTL); "fresher" is 40ms
        // old (still within it).
        let mut keys = index.keys().await;
        keys.sort_unstable();
        assert_eq!(keys, vec!["fresher"]);
    }

    #[tokio::test]
    async fn an_entry_expires_after_its_ttl_even_without_an_explicit_remove() {
        let index: EmbeddingIndex<&str> = EmbeddingIndex::new(Some(Duration::from_millis(50)));
        index.insert("temp", embedding(&[1.0, 0.0])).await;
        assert_eq!(index.len().await, 1);

        tokio::time::sleep(Duration::from_millis(200)).await;

        assert!(index.is_empty().await);
        assert!(index.nearest(&embedding(&[1.0, 0.0])).await.is_none());
    }

    #[tokio::test]
    async fn inserting_the_same_key_again_resets_its_ttl() {
        let index: EmbeddingIndex<&str> = EmbeddingIndex::new(Some(Duration::from_millis(100)));
        index.insert("key", embedding(&[1.0, 0.0])).await;

        tokio::time::sleep(Duration::from_millis(70)).await;
        index.insert("key", embedding(&[1.0, 0.0])).await;
        tokio::time::sleep(Duration::from_millis(70)).await;

        // 140ms since the first insert (which would have expired a
        // 100ms TTL), but only 70ms since the second, TTL-resetting one.
        assert_eq!(index.len().await, 1);
    }
}
