use std::borrow::Cow;

use axum::body::Bytes;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::value::RawValue;
use uuid::Uuid;

pub const MAX_TOPIC_LEN: usize = 255;

const UUID_LEN: usize = 16;

#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ServerMessage<'a> {
    Welcome {
        id: Uuid,
    },
    Subscribed {
        topic: &'a str,
    },
    Unsubscribed {
        topic: &'a str,
    },
    Message {
        topic: &'a str,
        from: Uuid,
        data: &'a RawValue,
    },
    Warning {
        dropped: u64,
    },
    Error {
        topic: Option<&'a str>,
        message: &'a str,
    },
}

#[derive(Debug, Deserialize)]
pub struct ClientFrame<'a> {
    #[serde(rename = "type", borrow)]
    pub kind: Cow<'a, str>,
    #[serde(borrow)]
    pub topic: Cow<'a, str>,
    #[serde(borrow, default, deserialize_with = "present")]
    pub data: Option<&'a RawValue>,
}

fn present<'de, D>(deserializer: D) -> Result<Option<&'de RawValue>, D::Error>
where
    D: Deserializer<'de>,
{
    <&RawValue>::deserialize(deserializer).map(Some)
}

pub fn is_valid_topic(topic: &str) -> bool {
    !topic.is_empty() && topic.len() <= MAX_TOPIC_LEN
}

#[derive(Debug, PartialEq, Eq)]
pub struct BinaryHeader<'a> {
    pub topic: &'a str,
    pub payload: &'a [u8],
}

impl<'a> BinaryHeader<'a> {
    pub fn parse(frame: &'a [u8]) -> Result<Self, Option<&'a str>> {
        let (&len, rest) = frame.split_first().ok_or(None)?;
        if len == 0 {
            return Err(Some(""));
        }
        let len = usize::from(len);
        if rest.len() < len {
            return Err(None);
        }
        let (topic, payload) = rest.split_at(len);
        let topic = std::str::from_utf8(topic).map_err(|_| None)?;
        Ok(Self { topic, payload })
    }

    pub fn delivered(&self, from: Uuid) -> Bytes {
        let mut frame = Vec::with_capacity(1 + self.topic.len() + UUID_LEN + self.payload.len());
        frame.push(self.topic.len() as u8);
        frame.extend_from_slice(self.topic.as_bytes());
        frame.extend_from_slice(from.as_bytes());
        frame.extend_from_slice(self.payload);
        Bytes::from(frame)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(text: &str) -> serde_json::Result<ClientFrame<'_>> {
        serde_json::from_str(text)
    }

    #[test]
    fn null_data_is_present() {
        let frame = parse(r#"{"type":"publish","topic":"k","data":null}"#).unwrap();
        assert_eq!(frame.data.map(RawValue::get), Some("null"));
    }

    #[test]
    fn absent_data_is_none() {
        let frame = parse(r#"{"type":"publish","topic":"k"}"#).unwrap();
        assert!(frame.data.is_none());
    }

    #[test]
    fn surrounding_whitespace_is_not_part_of_data() {
        let frame = parse(r#"{"type":"publish","topic":"k","data":  42  }"#).unwrap();
        assert_eq!(frame.data.map(RawValue::get), Some("42"));
    }

    #[test]
    fn data_bytes_are_kept() {
        let data = r#"{"b":1,  "a":[ 1,2 ],"u":"\u00e9","n":1.50}"#;
        let text = format!(r#"{{"type":"publish","topic":"k","data":{data}}}"#);
        let frame = parse(&text).unwrap();
        assert_eq!(frame.data.map(RawValue::get), Some(data));
    }

    #[test]
    fn duplicate_fields_are_rejected() {
        assert!(parse(r#"{"type":"subscribe","topic":"a","topic":"b"}"#).is_err());
    }

    #[test]
    fn escaped_type_and_topic_are_unescaped() {
        let frame = parse(r#"{"type":"\u0070ublish","topic":"\u00e9","data":1}"#).unwrap();
        assert_eq!(frame.kind, "publish");
        assert_eq!(frame.topic, "é");
        assert!(matches!(frame.topic, Cow::Owned(_)));
    }

    #[test]
    fn unescaped_topic_is_borrowed() {
        let frame = parse(r#"{"type":"subscribe","topic":"k"}"#).unwrap();
        assert!(matches!(frame.topic, Cow::Borrowed("k")));
        assert!(matches!(frame.kind, Cow::Borrowed("subscribe")));
    }

    #[test]
    fn missing_or_non_string_topic_is_rejected() {
        assert!(parse(r#"{"type":"subscribe"}"#).is_err());
        assert!(parse(r#"{"type":"subscribe","topic":5}"#).is_err());
    }

    #[test]
    fn extra_fields_are_ignored() {
        assert!(parse(r#"{"type":"subscribe","topic":"k","extra":[1]}"#).is_ok());
    }

    #[test]
    fn binary_header_parses_topic_and_payload() {
        let header = BinaryHeader::parse(&[1, b'k', 0, 0xff, 0x10, 0x42]).unwrap();
        assert_eq!(header.topic, "k");
        assert_eq!(header.payload, &[0, 0xff, 0x10, 0x42]);
    }

    #[test]
    fn binary_header_accepts_empty_payload() {
        let header = BinaryHeader::parse(&[1, b'k']).unwrap();
        assert!(header.payload.is_empty());
    }

    #[test]
    fn binary_header_errors_carry_the_readable_topic() {
        assert_eq!(BinaryHeader::parse(&[]), Err(None));
        assert_eq!(BinaryHeader::parse(&[0, 0xff]), Err(Some("")));
        assert_eq!(BinaryHeader::parse(&[5, b'k']), Err(None));
        assert_eq!(BinaryHeader::parse(&[2, 0xff, 0xfe]), Err(None));
    }

    #[test]
    fn delivered_binary_inserts_sender_after_topic() {
        let from = Uuid::from_u128(7);
        let header = BinaryHeader::parse(&[1, b'k', 9, 8]).unwrap();
        let frame = header.delivered(from);
        let mut expected = vec![1, b'k'];
        expected.extend_from_slice(from.as_bytes());
        expected.extend_from_slice(&[9, 8]);
        assert_eq!(&frame[..], &expected[..]);
    }

    #[test]
    fn topic_length_bounds() {
        assert!(!is_valid_topic(""));
        assert!(is_valid_topic(&"x".repeat(MAX_TOPIC_LEN)));
        assert!(!is_valid_topic(&"x".repeat(MAX_TOPIC_LEN + 1)));
    }
}
