use serde::Serialize;
use serde_json::value::RawValue;
use uuid::Uuid;

#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ServerMessage<'a> {
    Welcome { id: Uuid },
    Message { from: Uuid, data: &'a RawValue },
    Warning { dropped: u64 },
    Error { message: &'a str },
}
