//! Word-stream → chunk slicer.
//!
//! Given a flat reading-order [`Word`] stream from [`crate::text_extractor`],
//! produce a sequence of [`ChunkOut`]s sized roughly to the configured
//! token budget, with the requested overlap. Each chunk carries:
//!
//! - the concatenated text (single space between words; line breaks as
//!   `\n` between visual lines),
//! - the (char_start, char_end) range it occupies inside its own text
//!   (always 0..text.len() for v1 — we don't reuse a single body string),
//! - a `bboxes: Vec<Bbox>` list, one rectangle per visual line the chunk
//!   spans, computed by grouping words by page + baseline (`y` within a
//!   small epsilon proportional to glyph height) and unioning each
//!   group's rectangles.
//!
//! The tokeniser is intentionally cheap (whitespace count). The design
//! doc says chunk size is a soft target; paragraph alignment dominates
//! retrieval quality anyway.

use crate::storage::Bbox;
use crate::text_extractor::Word;

#[derive(Debug, Clone, Copy)]
pub struct ChunkConfig {
    /// Target token count per chunk. Tokens are "whitespace-split words"
    /// for v1 — cheap and within a constant factor of any real
    /// tokeniser for sizing purposes.
    pub size_tokens: usize,
    /// Overlap (in tokens) between adjacent chunks. Both chunks see the
    /// same `overlap_tokens` worth of words at the boundary.
    pub overlap_tokens: usize,
    /// String written to `chunk.chunk_strategy`, e.g. `"v1-fixed-overlap"`.
    /// Stored so a re-ingest with a different strategy doesn't collide
    /// with old rows (the `chunk_unique` index keys on it).
    pub strategy: &'static str,
}

impl Default for ChunkConfig {
    fn default() -> Self {
        Self {
            size_tokens: 400,
            overlap_tokens: 50,
            strategy: "v1-fixed-overlap",
        }
    }
}

/// One chunk emitted by [`chunk_words`]. The caller embeds `text` and
/// attaches the resulting vector before persisting.
#[derive(Debug, Clone)]
pub struct ChunkOut {
    pub ordinal: i64,
    pub text: String,
    pub char_start: i64,
    pub char_end: i64,
    pub bboxes: Vec<Bbox>,
}

/// Split a reading-order [`Word`] stream into overlapping fixed-size
/// chunks. `cfg.size_tokens` and `cfg.overlap_tokens` apply; on input
/// shorter than `size_tokens`, exactly one chunk is emitted.
pub fn chunk_words(words: &[Word], cfg: ChunkConfig) -> Vec<ChunkOut> {
    if words.is_empty() {
        return Vec::new();
    }
    let size = cfg.size_tokens.max(1);
    let overlap = cfg.overlap_tokens.min(size.saturating_sub(1));
    let step = size - overlap;

    let mut out: Vec<ChunkOut> = Vec::new();
    let mut ord: i64 = 0;
    let mut start = 0usize;
    loop {
        let end = (start + size).min(words.len());
        let slice = &words[start..end];
        let (text, bboxes) = render_slice(slice);
        let len = text.len() as i64;
        out.push(ChunkOut {
            ordinal: ord,
            text,
            char_start: 0,
            char_end: len,
            bboxes,
        });
        ord += 1;
        if end >= words.len() {
            break;
        }
        start += step;
    }
    out
}

/// Concatenate a slice of words into a single chunk's text + per-line
/// bboxes. Words on the same page + baseline are joined with single
/// spaces, lines separated by `\n`. The bbox for each line is the union
/// of its constituent word rectangles.
fn render_slice(words: &[Word]) -> (String, Vec<Bbox>) {
    let mut text = String::new();
    let mut bboxes: Vec<Bbox> = Vec::new();
    let mut iter = words.iter().peekable();
    let mut first_line = true;
    while let Some(first) = iter.next() {
        // The line group: this word plus any following word on the same
        // page whose `y` is within an epsilon proportional to glyph
        // height. (PDF coords are bottom-left, so two words on the same
        // baseline have similar `y` values — the bottom edge of their
        // box.)
        let baseline = first.y;
        let eps = (first.h.max(1.0)) * 0.5;
        let mut line_words: Vec<&Word> = vec![first];
        while let Some(next) = iter.peek() {
            if next.page == first.page && (next.y - baseline).abs() <= eps {
                line_words.push(iter.next().unwrap());
            } else {
                break;
            }
        }
        // Emit the line's text.
        if !first_line {
            text.push('\n');
        }
        first_line = false;
        for (i, w) in line_words.iter().enumerate() {
            if i > 0 {
                text.push(' ');
            }
            text.push_str(&w.text);
        }
        // Union of word rectangles → one line bbox.
        let (mut x_min, mut y_min, mut x_max, mut y_max) =
            (f64::INFINITY, f64::INFINITY, f64::NEG_INFINITY, f64::NEG_INFINITY);
        for w in &line_words {
            x_min = x_min.min(w.x);
            y_min = y_min.min(w.y);
            x_max = x_max.max(w.x + w.w);
            y_max = y_max.max(w.y + w.h);
        }
        bboxes.push(Bbox {
            page: first.page,
            x: x_min,
            y: y_min,
            w: (x_max - x_min).max(0.0),
            h: (y_max - y_min).max(0.0),
        });
    }
    (text, bboxes)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn word(page: i64, x: f64, y: f64, text: &str) -> Word {
        Word {
            page,
            x,
            y,
            w: 10.0 * text.len() as f64,
            h: 12.0,
            text: text.into(),
        }
    }

    #[test]
    fn empty_input_yields_no_chunks() {
        let out = chunk_words(&[], ChunkConfig::default());
        assert!(out.is_empty());
    }

    #[test]
    fn short_input_yields_one_chunk() {
        let words = vec![word(1, 0.0, 700.0, "hello"), word(1, 60.0, 700.0, "world")];
        let out = chunk_words(
            &words,
            ChunkConfig {
                size_tokens: 100,
                overlap_tokens: 10,
                strategy: "test",
            },
        );
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].text, "hello world");
        assert_eq!(out[0].bboxes.len(), 1);
        assert_eq!(out[0].bboxes[0].page, 1);
    }

    #[test]
    fn respects_size_and_overlap() {
        // 25 words, size=10, overlap=2 → step=8 → starts 0,8,16 → 3 chunks.
        let words: Vec<Word> = (0..25)
            .map(|i| word(1, i as f64 * 12.0, 700.0, &format!("w{i}")))
            .collect();
        let out = chunk_words(
            &words,
            ChunkConfig {
                size_tokens: 10,
                overlap_tokens: 2,
                strategy: "test",
            },
        );
        assert_eq!(out.len(), 3);
        // First chunk: w0..w9
        assert!(out[0].text.starts_with("w0 "));
        assert!(out[0].text.ends_with(" w9"));
        // Second chunk starts at index 8 (overlap of 2 with the first):
        assert!(out[1].text.starts_with("w8 "));
    }

    #[test]
    fn groups_words_by_baseline_into_lines() {
        // Two lines on page 1: y=700 and y=680. The chunker should
        // union each line's words into one bbox.
        let words = vec![
            word(1, 0.0, 700.0, "alpha"),
            word(1, 60.0, 700.0, "beta"),
            word(1, 0.0, 680.0, "gamma"),
            word(1, 60.0, 680.0, "delta"),
        ];
        let out = chunk_words(&words, ChunkConfig::default());
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].text, "alpha beta\ngamma delta");
        assert_eq!(out[0].bboxes.len(), 2);
        assert!((out[0].bboxes[0].y - 700.0).abs() < 0.01);
        assert!((out[0].bboxes[1].y - 680.0).abs() < 0.01);
        // Union width covers both words on each line.
        assert!(out[0].bboxes[0].w > 60.0);
    }

    #[test]
    fn multipage_chunk_keeps_bboxes_from_both_pages() {
        // Two pages, two words each. With a size big enough they end up
        // in the same chunk; bboxes should mention page=1 and page=2.
        let words = vec![
            word(1, 0.0, 700.0, "p1a"),
            word(1, 50.0, 700.0, "p1b"),
            word(2, 0.0, 700.0, "p2a"),
            word(2, 50.0, 700.0, "p2b"),
        ];
        let out = chunk_words(
            &words,
            ChunkConfig {
                size_tokens: 100,
                overlap_tokens: 0,
                strategy: "test",
            },
        );
        assert_eq!(out.len(), 1);
        let pages: Vec<i64> = out[0].bboxes.iter().map(|b| b.page).collect();
        assert!(pages.contains(&1));
        assert!(pages.contains(&2));
    }

    #[test]
    fn ordinals_start_at_zero_and_increment() {
        let words: Vec<Word> = (0..30)
            .map(|i| word(1, i as f64 * 12.0, 700.0, &format!("w{i}")))
            .collect();
        let out = chunk_words(
            &words,
            ChunkConfig {
                size_tokens: 10,
                overlap_tokens: 0,
                strategy: "test",
            },
        );
        for (i, c) in out.iter().enumerate() {
            assert_eq!(c.ordinal, i as i64);
        }
    }
}
