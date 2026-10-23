//! Decoders for fields a reader keeps only as diagnostics.
//!
//! A field added to a record's decoded shape must never make a record that
//! decoded before fail to decode now: that would drop or set aside usage the
//! ledger already holds. These decoders accept any JSON value and keep it
//! only when it has the expected type, so a record whose diagnostic field is
//! malformed still decodes exactly as it did before the field was read.

use serde::de::{Deserializer, IgnoredAny, MapAccess, SeqAccess, Visitor};
use std::fmt;
use std::marker::PhantomData;

/// What a lenient field keeps from one JSON value.
trait Keep: Sized {
    fn from_str(_value: &str) -> Option<Self> {
        None
    }
    fn from_u64(_value: u64) -> Option<Self> {
        None
    }
    fn from_i64(_value: i64) -> Option<Self> {
        None
    }
}

impl Keep for String {
    fn from_str(value: &str) -> Option<Self> {
        Some(value.to_string())
    }
}

impl Keep for u64 {
    fn from_u64(value: u64) -> Option<Self> {
        Some(value)
    }
    fn from_i64(value: i64) -> Option<Self> {
        u64::try_from(value).ok()
    }
}

struct Lenient<T>(PhantomData<T>);

impl<'de, T: Keep> Visitor<'de> for Lenient<T> {
    type Value = Option<T>;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("any JSON value")
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E> {
        Ok(T::from_str(value))
    }

    fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
        Ok(T::from_u64(value))
    }

    fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E> {
        Ok(T::from_i64(value))
    }

    fn visit_f64<E>(self, _value: f64) -> Result<Self::Value, E> {
        Ok(None)
    }

    fn visit_bool<E>(self, _value: bool) -> Result<Self::Value, E> {
        Ok(None)
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E> {
        Ok(None)
    }

    fn visit_none<E>(self) -> Result<Self::Value, E> {
        Ok(None)
    }

    fn visit_some<D: Deserializer<'de>>(self, deserializer: D) -> Result<Self::Value, D::Error> {
        deserializer.deserialize_any(self)
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
        while seq.next_element::<IgnoredAny>()?.is_some() {}
        Ok(None)
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
        while map.next_entry::<IgnoredAny, IgnoredAny>()?.is_some() {}
        Ok(None)
    }
}

/// A string, or `None` for any other JSON value.
pub(crate) fn string<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<String>, D::Error> {
    deserializer.deserialize_any(Lenient::<String>(PhantomData))
}

/// A non-negative integer, or `None` for any other JSON value.
pub(crate) fn unsigned<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<u64>, D::Error> {
    deserializer.deserialize_any(Lenient::<u64>(PhantomData))
}

#[cfg(test)]
mod tests {
    use serde::Deserialize;

    #[derive(Deserialize)]
    struct Probe {
        #[serde(default, deserialize_with = "super::string")]
        text: Option<String>,
        #[serde(default, deserialize_with = "super::unsigned")]
        number: Option<u64>,
        after: Option<u64>,
    }

    fn probe(json: &str) -> (Option<String>, Option<u64>, Option<u64>) {
        let mut bytes = json.as_bytes().to_vec();
        let simd: Probe = simd_json::from_slice(&mut bytes).unwrap();
        let serde: Probe = serde_json::from_str(json).unwrap();
        assert_eq!(
            (&simd.text, simd.number, simd.after),
            (&serde.text, serde.number, serde.after)
        );
        (serde.text, serde.number, serde.after)
    }

    /// Every JSON shape decodes, the expected type is kept, anything else
    /// becomes `None`, and the fields after it still decode.
    #[test]
    fn any_value_decodes_and_only_the_expected_type_is_kept() {
        assert_eq!(
            probe(r#"{"text":"stop","number":258400,"after":1}"#),
            (Some("stop".into()), Some(258_400), Some(1))
        );
        assert_eq!(probe(r#"{"after":1}"#), (None, None, Some(1)));
        assert_eq!(
            probe(r#"{"text":null,"number":null,"after":1}"#),
            (None, None, Some(1))
        );
        assert_eq!(
            probe(r#"{"text":7,"number":-3,"after":1}"#),
            (None, None, Some(1))
        );
        assert_eq!(
            probe(r#"{"text":true,"number":1.5,"after":1}"#),
            (None, None, Some(1))
        );
        assert_eq!(
            probe(r#"{"text":{"a":[1,{"b":2}]},"number":[1,"x",{"c":null}],"after":1}"#),
            (None, None, Some(1))
        );
        assert_eq!(
            probe(r#"{"text":"","number":0,"after":1}"#),
            (Some(String::new()), Some(0), Some(1))
        );
    }
}
