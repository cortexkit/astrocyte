//! Astrocyte's internal canonical JSON format + SHA-256 + lowercase hex.
//!
//! One hashing discipline everywhere: event identity, charge ids and payload
//! hashes all use the same stack. The format borrows UTF-16 member ordering
//! and string escaping from RFC 8785, but writes signed and unsigned 64-bit
//! integers exactly, including beyond binary64's exact range. It is NOT RFC
//! 8785/JCS: changing integer serialization would change stored identities.
//! Floats are forbidden and return [`CanonError::FloatInCanonicalForm`].

use serde_json::Value;
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CanonError {
    /// The canonical form contained a non-integer number. The design's
    /// canonical payloads are float-free by construction; hitting this means a
    /// caller put a float where none belongs.
    FloatInCanonicalForm,
}

/// Serialize Astrocyte's internal, exact-integer canonical JSON format.
/// The historical function name does not imply RFC 8785 number serialization.
pub fn jcs_serialize(value: &Value) -> Result<String, CanonError> {
    let mut out = String::new();
    write_value(value, &mut out)?;
    Ok(out)
}

fn write_value(value: &Value, out: &mut String) -> Result<(), CanonError> {
    match value {
        Value::Null => out.push_str("null"),
        Value::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                out.push_str(&i.to_string());
            } else if let Some(u) = n.as_u64() {
                out.push_str(&u.to_string());
            } else {
                return Err(CanonError::FloatInCanonicalForm);
            }
        }
        Value::String(s) => write_string(s, out),
        Value::Array(items) => {
            out.push('[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_value(item, out)?;
            }
            out.push(']');
        }
        Value::Object(map) => {
            // A supplementary character's leading surrogate sorts before BMP
            // characters in U+E000..U+FFFF, unlike UTF-8 byte ordering. Compare
            // UTF-16 iterators so sorting does not allocate comparison keys.
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort_by(|a, b| a.encode_utf16().cmp(b.encode_utf16()));
            out.push('{');
            for (i, key) in keys.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_string(key, out);
                out.push(':');
                write_value(&map[key.as_str()], out)?;
            }
            out.push('}');
        }
    }
    Ok(())
}

/// RFC 8785 string serialization: the two-character escapes for the usual
/// suspects, \u00XX for other control characters, everything else literal.
fn write_string(s: &str, out: &mut String) {
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\u{0008}' => out.push_str("\\b"),
            '\u{000C}' => out.push_str("\\f"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => {
                out.push_str(&format!("\\u{:04x}", c as u32));
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

/// SHA-256 over the internal canonical form, encoded as lowercase hex.
pub fn canonical_hash(value: &Value) -> Result<String, CanonError> {
    let canon = jcs_serialize(value)?;
    let mut hasher = Sha256::new();
    hasher.update(canon.as_bytes());
    Ok(hex::encode(hasher.finalize()))
}

/// Hash a small object of distinct string fields without owning a JSON tree.
/// Sorting and escaping match `jcs_serialize`. Only the supplied fields are
/// written; callers omit absent fields from the array rather than use a sentinel.
pub(crate) fn canonical_string_object_hash<const N: usize>(
    mut fields: [(&str, &str); N],
) -> String {
    fields.sort_by(|a, b| a.0.encode_utf16().cmp(b.0.encode_utf16()));
    let mut out = String::new();
    out.push('{');
    for (i, (key, value)) in fields.into_iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        write_string(key, &mut out);
        out.push(':');
        write_string(value, &mut out);
    }
    out.push('}');
    hex::encode(Sha256::digest(out.as_bytes()))
}

#[cfg(test)]
pub(crate) mod allocation_probe {
    use std::alloc::{GlobalAlloc, Layout, System};
    use std::cell::Cell;

    // Count only the probe's thread while enabled, so parallel tests and their
    // fixtures cannot contribute to the measured allocation/reallocation calls.
    thread_local! {
        static CALLS: Cell<Option<usize>> = const { Cell::new(None) };
    }

    struct CountingAllocator;

    fn record() {
        let _ = CALLS.try_with(|calls| {
            if let Some(count) = calls.get() {
                calls.set(Some(count + 1));
            }
        });
    }

    unsafe impl GlobalAlloc for CountingAllocator {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            record();
            unsafe { System.alloc(layout) }
        }

        unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
            record();
            unsafe { System.alloc_zeroed(layout) }
        }

        unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
            record();
            unsafe { System.realloc(pointer, layout, size) }
        }

        unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
            unsafe { System.dealloc(pointer, layout) }
        }
    }

    #[global_allocator]
    static ALLOCATOR: CountingAllocator = CountingAllocator;

    pub(crate) fn count(run: impl FnOnce()) -> usize {
        struct Reset;
        impl Drop for Reset {
            fn drop(&mut self) {
                CALLS.with(|calls| calls.set(None));
            }
        }
        CALLS.with(|calls| assert!(calls.replace(Some(0)).is_none(), "nested allocation probe"));
        let _reset = Reset;
        run();
        CALLS.with(|calls| calls.get().unwrap())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn sorts_keys_and_is_deterministic() {
        let a = json!({"b": 1, "a": 2, "c": {"z": 1, "y": 2}});
        let s = jcs_serialize(&a).unwrap();
        assert_eq!(s, r#"{"a":2,"b":1,"c":{"y":2,"z":1}}"#);
        assert_eq!(canonical_hash(&a).unwrap(), canonical_hash(&a).unwrap());
    }

    #[test]
    fn rejects_floats() {
        let v = json!({"x": 1.5});
        assert_eq!(jcs_serialize(&v), Err(CanonError::FloatInCanonicalForm));
    }

    #[test]
    fn escapes_and_hex_shape() {
        let v = json!({"s": "a\"b\\c\nd\u{0001}"});
        assert_eq!(
            jcs_serialize(&v).unwrap(),
            "{\"s\":\"a\\\"b\\\\c\\nd\\u0001\"}"
        );
        let h = canonical_hash(&v).unwrap();
        assert_eq!(h.len(), 64);
        assert!(h
            .chars()
            .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase()));
    }

    #[test]
    fn utf16_ordering_rule() {
        // RFC 8785's example class: keys sort by UTF-16 code units. A
        // supplementary-plane char (surrogate pair, first unit 0xD800-range)
        // sorts BELOW U+E000 (private use, single unit).
        let v = json!({"\u{10000}": 1, "\u{E000}": 2});
        let s = jcs_serialize(&v).unwrap();
        let pos_supp = s.find('\u{10000}').unwrap();
        let pos_priv = s.find('\u{E000}').unwrap();
        assert!(pos_supp < pos_priv);
    }

    #[test]
    fn exact_integer_bytes_are_preserved() {
        let value =
            json!({"max": u64::MAX, "min": i64::MIN, "beyond_double": 9_007_199_254_740_993_u64});
        assert_eq!(
            jcs_serialize(&value).unwrap(),
            r#"{"beyond_double":9007199254740993,"max":18446744073709551615,"min":-9223372036854775808}"#
        );
    }

    #[test]
    #[ignore = "wall-time probe; run with --ignored --nocapture --test-threads=1"]
    fn performance_canonical_key_sorting() {
        let value = Value::Object(
            (0..128)
                .map(|i| (format!("\u{10000}{i:04}"), json!(i)))
                .collect(),
        );
        let start = std::time::Instant::now();
        let allocations = allocation_probe::count(|| {
            for _ in 0..10_000 {
                std::hint::black_box(jcs_serialize(std::hint::black_box(&value)).unwrap());
            }
        });
        println!(
            "canonical sorting: 10000 serializations of 128 keys: {allocations} allocation/reallocation calls, {:?}",
            start.elapsed()
        );
        assert!(
            allocations <= 1_400_000,
            "canonical sorting must not allocate UTF-16 comparison keys: {allocations}"
        );
    }
}
