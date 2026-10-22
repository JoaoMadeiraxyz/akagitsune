use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async,
    tungstenite::{
        Error as WsError, Message as WsMessage, client::IntoClientRequest, http::Response,
    },
};

type Client = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

async fn spawn_server() -> String {
    spawn_server_with_tokens("").await
}

async fn spawn_server_with_tokens(tokens: &str) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let tokens = realtime_gateway::auth::parse_tokens(tokens).unwrap();
    tokio::spawn(realtime_gateway::run(
        listener,
        tokens,
        std::future::pending(),
    ));
    format!("ws://{addr}/ws")
}

async fn next_msg(client: &mut Client) -> WsMessage {
    tokio::time::timeout(Duration::from_secs(5), client.next())
        .await
        .expect("timed out waiting for a message")
        .expect("stream closed")
        .unwrap()
}

async fn send(client: &mut Client, msg: WsMessage) {
    tokio::time::timeout(Duration::from_secs(5), client.send(msg))
        .await
        .expect("timed out sending a frame")
        .unwrap()
}

async fn next_json(client: &mut Client) -> Value {
    serde_json::from_str(&next_msg(client).await.into_text().unwrap()).unwrap()
}

async fn assert_silent(client: &mut Client) {
    let unexpected = tokio::time::timeout(Duration::from_millis(200), client.next()).await;
    assert!(unexpected.is_err(), "unexpected frame: {unexpected:?}");
}

fn json_string_of_len(len: usize) -> String {
    format!("\"{}\"", "x".repeat(len - 2))
}

fn publish_text(topic: &str, data: &str) -> String {
    format!(
        r#"{{"type":"publish","topic":{},"data":{data}}}"#,
        json!(topic)
    )
}

fn message_json(topic: &str, from: &str, data: Value) -> Value {
    json!({"type": "message", "topic": topic, "from": from, "data": data})
}

fn binary_frame(topic: &str, payload: &[u8]) -> Vec<u8> {
    let mut frame = vec![topic.len() as u8];
    frame.extend_from_slice(topic.as_bytes());
    frame.extend_from_slice(payload);
    frame
}

async fn subscribe_request(client: &mut Client, topic: &str) {
    send(
        client,
        WsMessage::text(json!({"type": "subscribe", "topic": topic}).to_string()),
    )
    .await;
}

async fn subscribe(client: &mut Client, topic: &str) {
    subscribe_request(client, topic).await;
    assert_eq!(
        next_json(client).await,
        json!({"type": "subscribed", "topic": topic})
    );
}

async fn unsubscribe(client: &mut Client, topic: &str) {
    send(
        client,
        WsMessage::text(json!({"type": "unsubscribe", "topic": topic}).to_string()),
    )
    .await;
    assert_eq!(
        next_json(client).await,
        json!({"type": "unsubscribed", "topic": topic})
    );
}

async fn publish(client: &mut Client, topic: &str, data: &str) {
    send(client, WsMessage::text(publish_text(topic, data))).await;
}

async fn next_error(client: &mut Client) -> Value {
    let msg = next_json(client).await;
    assert_eq!(msg["type"], "error", "{msg}");
    msg
}

async fn connect(url: &str) -> (Client, String) {
    let (mut client, _) = connect_async(url).await.unwrap();
    let welcome = next_json(&mut client).await;
    assert_eq!(welcome["type"], "welcome");
    let id = welcome["id"].as_str().unwrap().to_string();
    (client, id)
}

#[tokio::test]
async fn connection_is_welcomed_with_an_id() {
    let url = spawn_server().await;
    let (_client, id) = connect(&url).await;
    assert!(uuid::Uuid::parse_str(&id).is_ok(), "not a uuid: {id}");
}

#[tokio::test]
async fn frame_at_size_limit_is_relayed() {
    let url = spawn_server().await;
    let (mut a, a_id) = connect(&url).await;
    let (mut b, _) = connect(&url).await;
    subscribe(&mut b, "k").await;

    let prefix = r#"{"type":"publish","topic":"k","data":"#;
    let data = json_string_of_len(64 * 1024 - prefix.len() - 1);
    let frame = format!("{prefix}{data}}}");
    assert_eq!(frame.len(), 64 * 1024);
    send(&mut a, WsMessage::text(frame)).await;

    assert_eq!(
        next_json(&mut b).await,
        message_json("k", &a_id, serde_json::from_str(&data).unwrap())
    );
}

#[tokio::test]
async fn oversized_frame_drops_the_sender_without_relaying() {
    let url = spawn_server().await;
    let (mut a, _) = connect(&url).await;
    let (mut b, _) = connect(&url).await;
    subscribe(&mut b, "k").await;

    let prefix = r#"{"type":"publish","topic":"k","data":"#;
    let data = json_string_of_len(64 * 1024 + 1 - prefix.len() - 1);
    let frame = format!("{prefix}{data}}}");
    assert_eq!(frame.len(), 64 * 1024 + 1);
    send(&mut a, WsMessage::text(frame)).await;

    match tokio::time::timeout(Duration::from_secs(5), a.next())
        .await
        .expect("sender connection is still open")
    {
        None | Some(Err(_)) => (),
        Some(Ok(WsMessage::Close(frame))) => panic!("unexpected close frame: {frame:?}"),
        Some(Ok(other)) => panic!("unexpected frame: {other:?}"),
    }
    assert_silent(&mut b).await;
}

#[tokio::test]
async fn payload_is_relayed_verbatim_to_others() {
    let url = spawn_server().await;
    let (mut a, a_id) = connect(&url).await;
    let (mut b, _) = connect(&url).await;
    subscribe(&mut b, "k").await;

    let payload = json!({"hp": 42, "pos": [1, 2], "nested": {"any": null}});
    publish(&mut a, "k", &payload.to_string()).await;

    assert_eq!(next_json(&mut b).await, message_json("k", &a_id, payload));
    assert_silent(&mut a).await;
}

#[tokio::test]
async fn text_payload_bytes_are_relayed_verbatim() {
    let url = spawn_server().await;
    let (mut a, a_id) = connect(&url).await;
    let (mut b, _) = connect(&url).await;
    subscribe(&mut b, "k").await;

    let payload = r#"{"b":1,  "a":[ 1,2 ],"u":"\u00e9","n":1.50}"#;
    publish(&mut a, "k", payload).await;
    assert_eq!(
        next_msg(&mut b).await.into_text().unwrap().as_str(),
        format!(r#"{{"type":"message","topic":"k","from":"{a_id}","data":{payload}}}"#)
    );

    publish(&mut a, "k", "  42  ").await;
    assert_eq!(
        next_msg(&mut b).await.into_text().unwrap().as_str(),
        format!(r#"{{"type":"message","topic":"k","from":"{a_id}","data":42}}"#)
    );
}

#[tokio::test]
async fn any_json_shape_is_accepted() {
    let url = spawn_server().await;
    let (mut a, _) = connect(&url).await;
    let (mut b, _) = connect(&url).await;
    subscribe(&mut b, "k").await;

    for payload in [json!(42), json!("texto"), json!([1, 2, 3]), json!(null)] {
        publish(&mut a, "k", &payload.to_string()).await;
        assert_eq!(next_json(&mut b).await["data"], payload);
    }
}

#[tokio::test]
async fn binary_frames_carry_topic_and_sender() {
    let url = spawn_server().await;
    let (mut a, a_id) = connect(&url).await;
    let (mut b, _) = connect(&url).await;
    subscribe(&mut b, "k").await;

    send(
        &mut a,
        WsMessage::binary(vec![0x01, 0x6b, 0x00, 0xff, 0x10, 0x42]),
    )
    .await;

    let mut expected = vec![0x01, 0x6b];
    expected.extend_from_slice(uuid::Uuid::parse_str(&a_id).unwrap().as_bytes());
    expected.extend_from_slice(&[0x00, 0xff, 0x10, 0x42]);
    assert_eq!(next_msg(&mut b).await, WsMessage::binary(expected.clone()));
    assert_silent(&mut a).await;

    send(&mut a, WsMessage::binary(vec![0x01, 0x6b])).await;
    expected.truncate(2 + 16);
    assert_eq!(next_msg(&mut b).await, WsMessage::binary(expected));
}

#[tokio::test]
async fn binary_and_text_share_topics() {
    let url = spawn_server().await;
    let (mut a, a_id) = connect(&url).await;
    let (mut b, _) = connect(&url).await;
    subscribe(&mut b, "k").await;

    publish(&mut a, "k", "1").await;
    send(&mut a, WsMessage::binary(binary_frame("k", b"raw"))).await;

    assert_eq!(next_json(&mut b).await, message_json("k", &a_id, json!(1)));
    assert!(matches!(next_msg(&mut b).await, WsMessage::Binary(_)));
}

#[tokio::test]
async fn malformed_binary_is_rejected() {
    let url = spawn_server().await;
    let (mut a, _) = connect(&url).await;
    let (mut b, _) = connect(&url).await;
    subscribe(&mut b, "k").await;

    send(&mut a, WsMessage::binary(vec![0x05, 0x6b])).await;
    let error = next_error(&mut a).await;
    assert_eq!(error["topic"], Value::Null);
    assert!(error["message"].as_str().is_some());

    send(&mut a, WsMessage::binary(vec![0x00, 0xff])).await;
    assert_eq!(next_error(&mut a).await["topic"], "");

    send(&mut a, WsMessage::binary(vec![0x02, 0xff, 0xfe, 0x01])).await;
    assert_eq!(next_error(&mut a).await["topic"], Value::Null);

    send(&mut a, WsMessage::binary(Vec::new())).await;
    assert_eq!(next_error(&mut a).await["topic"], Value::Null);

    assert_silent(&mut b).await;

    publish(&mut a, "k", "1").await;
    assert_eq!(next_json(&mut b).await["data"], json!(1));
}

#[tokio::test]
async fn fifo_order_holds_across_a_multi_batch_burst() {
    let url = spawn_server().await;
    let (mut a, _) = connect(&url).await;
    let (mut b, _) = connect(&url).await;
    subscribe(&mut b, "k").await;

    let burst = 200;
    for i in 0..burst {
        publish(&mut a, "k", &json!({ "seq": i }).to_string()).await;
    }

    for i in 0..burst {
        assert_eq!(next_json(&mut b).await["data"], json!({ "seq": i }));
    }
}

#[tokio::test]
async fn order_holds_across_topics() {
    let url = spawn_server().await;
    let (mut a, _) = connect(&url).await;
    let (mut b, _) = connect(&url).await;
    subscribe(&mut b, "k1").await;
    subscribe(&mut b, "k2").await;

    let burst = 200;
    for i in 0..burst {
        let topic = if i % 2 == 0 { "k1" } else { "k2" };
        publish(&mut a, topic, &json!({ "seq": i }).to_string()).await;
    }

    for i in 0..burst {
        assert_eq!(next_json(&mut b).await["data"], json!({ "seq": i }));
    }
}

#[tokio::test]
async fn slow_consumer_receives_a_warning_frame() {
    let url = spawn_server().await;
    let (mut a, _) = connect(&url).await;
    let (mut b, _) = connect(&url).await;
    subscribe(&mut b, "k").await;

    for i in 0..128_000 {
        publish(&mut a, "k", &json!({ "seq": i }).to_string()).await;
    }

    let warning = loop {
        let msg = next_json(&mut b).await;
        if msg["type"] == "warning" {
            break msg;
        }
    };
    assert!(warning["dropped"].as_u64().unwrap() > 0);
}

#[tokio::test]
async fn delivery_resumes_after_a_warning() {
    let url = spawn_server().await;
    let (mut a, _) = connect(&url).await;
    let (mut b, _) = connect(&url).await;
    subscribe(&mut b, "k").await;

    for i in 0..128_000 {
        publish(&mut a, "k", &json!({ "seq": i }).to_string()).await;
    }
    publish(&mut a, "k", &json!({ "marker": "end" }).to_string()).await;

    let mut warnings = 0;
    let mut last_seq: Option<u64> = None;
    loop {
        let msg = next_json(&mut b).await;
        match msg["type"].as_str() {
            Some("warning") => {
                assert!(msg["dropped"].as_u64().unwrap() > 0, "{msg}");
                warnings += 1;
            }
            Some("message") if msg["data"] == json!({ "marker": "end" }) => break,
            Some("message") => {
                let seq = msg["data"]["seq"].as_u64().unwrap();
                if let Some(prev) = last_seq {
                    assert!(seq > prev, "seq {seq} arrived after {prev}");
                }
                last_seq = Some(seq);
            }
            _ => panic!("unexpected frame: {msg}"),
        }
    }
    assert!(
        warnings > 0,
        "receiver never lagged, so resumption was not exercised"
    );
}

#[tokio::test]
async fn dropped_count_matches_the_frames_skipped() {
    let url = spawn_server().await;
    let (mut a, _) = connect(&url).await;
    let (mut b, _) = connect(&url).await;
    subscribe(&mut b, "k").await;

    let total: u64 = 128_000;
    for i in 0..total {
        publish(&mut a, "k", &json!({ "seq": i }).to_string()).await;
    }
    publish(&mut a, "k", &json!({ "marker": "end" }).to_string()).await;

    let mut warnings = 0;
    let mut received: u64 = 0;
    let mut dropped_total: u64 = 0;
    let mut dropped_since_last: u64 = 0;
    let mut expected_next: u64 = 0;
    loop {
        let msg = next_json(&mut b).await;
        if msg["type"] == "warning" {
            let dropped = msg["dropped"].as_u64().unwrap();
            dropped_since_last += dropped;
            dropped_total += dropped;
            warnings += 1;
            continue;
        }
        received += 1;
        if msg["data"] == json!({ "marker": "end" }) {
            break;
        }
        let seq = msg["data"]["seq"].as_u64().unwrap();
        assert_eq!(
            seq,
            expected_next + dropped_since_last,
            "seq {seq} after {dropped_since_last} reported dropped"
        );
        expected_next = seq + 1;
        dropped_since_last = 0;
    }
    assert!(
        warnings > 0,
        "receiver never lagged, so the count was not exercised"
    );
    assert_eq!(received + dropped_total, total + 1);
}

#[tokio::test]
async fn slow_receiver_does_not_hold_back_others() {
    let url = spawn_server().await;
    let (mut a, _) = connect(&url).await;
    let (mut b, _) = connect(&url).await;
    let (mut c, _) = connect(&url).await;
    subscribe(&mut b, "k").await;
    subscribe(&mut c, "k").await;

    let chunk = 100;
    for start in (0..128_000).step_by(chunk) {
        for i in start..start + chunk {
            publish(&mut a, "k", &json!({ "seq": i }).to_string()).await;
        }
        for i in start..start + chunk {
            let msg = next_json(&mut c).await;
            assert_eq!(msg["data"], json!({ "seq": i }), "{msg}");
        }
    }
    publish(&mut a, "k", &json!({ "marker": "end" }).to_string()).await;
    assert_eq!(next_json(&mut c).await["data"], json!({ "marker": "end" }));

    loop {
        let msg = next_json(&mut b).await;
        if msg["type"] == "warning" {
            break;
        }
        assert_ne!(
            msg["data"],
            json!({ "marker": "end" }),
            "the idle receiver never fell behind, so the test exercised nothing"
        );
    }
}

#[tokio::test]
async fn invalid_json_is_rejected_without_broadcasting() {
    let url = spawn_server().await;
    let (mut a, _) = connect(&url).await;
    let (mut b, _) = connect(&url).await;
    subscribe(&mut b, "k").await;

    send(&mut a, WsMessage::text("not json at all")).await;

    let error = next_error(&mut a).await;
    assert_eq!(error["topic"], Value::Null);
    assert_silent(&mut b).await;

    publish(&mut a, "k", &json!({"ok": true}).to_string()).await;
    assert_eq!(next_json(&mut b).await["data"], json!({"ok": true}));
}

#[tokio::test]
async fn unrecognized_frame_is_rejected() {
    let url = spawn_server().await;
    let (mut a, _) = connect(&url).await;
    let (mut b, _) = connect(&url).await;
    subscribe(&mut b, "k").await;

    send(&mut a, WsMessage::text(json!({"hp": 42}).to_string())).await;
    assert_eq!(next_error(&mut a).await["topic"], Value::Null);

    send(
        &mut a,
        WsMessage::text(json!({"type": "publish", "topic": "k"}).to_string()),
    )
    .await;
    assert_eq!(next_error(&mut a).await["topic"], "k");

    send(
        &mut a,
        WsMessage::text(json!({"type": "join", "topic": "k"}).to_string()),
    )
    .await;
    assert_eq!(next_error(&mut a).await["topic"], "k");

    assert_silent(&mut b).await;

    publish(&mut a, "k", "1").await;
    assert_eq!(next_json(&mut b).await["data"], json!(1));
}

#[tokio::test]
async fn subscribed_sender_gets_no_echo() {
    let url = spawn_server().await;
    let (mut a, a_id) = connect(&url).await;
    let (mut b, _) = connect(&url).await;
    subscribe(&mut a, "k").await;
    subscribe(&mut b, "k").await;

    publish(&mut a, "k", "1").await;
    send(&mut a, WsMessage::binary(binary_frame("k", b"raw"))).await;

    assert_eq!(next_json(&mut b).await, message_json("k", &a_id, json!(1)));
    assert!(matches!(next_msg(&mut b).await, WsMessage::Binary(_)));
    assert_silent(&mut a).await;
}

#[tokio::test]
async fn subscribe_is_acknowledged() {
    let url = spawn_server().await;
    let (mut a, _) = connect(&url).await;
    subscribe(&mut a, "k").await;
}

#[tokio::test]
async fn duplicate_subscribe_delivers_once() {
    let url = spawn_server().await;
    let (mut a, a_id) = connect(&url).await;
    let (mut b, _) = connect(&url).await;
    subscribe(&mut b, "k").await;
    subscribe(&mut b, "k").await;

    publish(&mut a, "k", "1").await;

    assert_eq!(next_json(&mut b).await, message_json("k", &a_id, json!(1)));
    assert_silent(&mut b).await;
}

#[tokio::test]
async fn publish_reaches_only_subscribers_of_its_topic() {
    let url = spawn_server().await;
    let (mut a, a_id) = connect(&url).await;
    let (mut b, _) = connect(&url).await;
    let (mut c, _) = connect(&url).await;
    let (mut d, _) = connect(&url).await;
    subscribe(&mut b, "k").await;
    subscribe(&mut c, "other").await;

    publish(&mut a, "k", r#"{"x":1}"#).await;

    assert_eq!(
        next_json(&mut b).await,
        message_json("k", &a_id, json!({"x": 1}))
    );
    assert_silent(&mut c).await;
    assert_silent(&mut d).await;
}

#[tokio::test]
async fn publisher_need_not_be_subscribed() {
    let url = spawn_server().await;
    let (mut a, a_id) = connect(&url).await;
    let (mut b, _) = connect(&url).await;
    subscribe(&mut b, "k").await;

    publish(&mut a, "k", "1").await;

    assert_eq!(next_json(&mut b).await, message_json("k", &a_id, json!(1)));
}

#[tokio::test]
async fn publish_to_empty_topic_is_silent() {
    let url = spawn_server().await;
    let (mut a, _) = connect(&url).await;

    publish(&mut a, "nobody", "1").await;
    assert_silent(&mut a).await;

    subscribe(&mut a, "alive").await;
}

#[tokio::test]
async fn publish_after_subscribed_is_delivered() {
    let url = spawn_server().await;
    let (mut a, a_id) = connect(&url).await;
    let (mut b, _) = connect(&url).await;

    for i in 0..50 {
        let topic = format!("k{i}");
        subscribe(&mut b, &topic).await;
        publish(&mut a, &topic, &i.to_string()).await;
        assert_eq!(
            next_json(&mut b).await,
            message_json(&topic, &a_id, json!(i))
        );
    }
}

#[tokio::test]
async fn unsubscribe_stops_delivery() {
    let url = spawn_server().await;
    let (mut a, a_id) = connect(&url).await;
    let (mut b, _) = connect(&url).await;
    subscribe(&mut b, "k").await;
    subscribe(&mut b, "probe").await;
    unsubscribe(&mut b, "k").await;

    publish(&mut a, "k", "1").await;
    publish(&mut a, "probe", r#""marker""#).await;

    assert_eq!(
        next_json(&mut b).await,
        message_json("probe", &a_id, json!("marker"))
    );
    assert_silent(&mut b).await;
}

#[tokio::test]
async fn unsubscribe_without_subscription_is_acknowledged() {
    let url = spawn_server().await;
    let (mut a, _) = connect(&url).await;
    unsubscribe(&mut a, "k").await;
}

#[tokio::test]
async fn disconnect_leaves_other_subscribers_working() {
    let url = spawn_server().await;
    let (mut a, _) = connect(&url).await;
    let (mut b, _) = connect(&url).await;
    let (mut c, _) = connect(&url).await;
    subscribe(&mut b, "k").await;
    subscribe(&mut c, "k").await;
    b.close(None).await.unwrap();
    drop(b);
    tokio::time::sleep(Duration::from_millis(100)).await;

    for i in 0..100 {
        publish(&mut a, "k", &json!({ "seq": i }).to_string()).await;
    }
    for i in 0..100 {
        assert_eq!(next_json(&mut c).await["data"], json!({ "seq": i }));
    }
}

#[tokio::test]
async fn publish_before_any_subscription_is_not_retained() {
    let url = spawn_server().await;
    let (mut a, a_id) = connect(&url).await;
    let (mut b, _) = connect(&url).await;

    publish(&mut a, "k", r#"{"seq":1}"#).await;
    subscribe(&mut b, "k").await;
    publish(&mut a, "k", r#"{"seq":2}"#).await;

    assert_eq!(
        next_json(&mut b).await,
        message_json("k", &a_id, json!({"seq": 2}))
    );
    assert_silent(&mut b).await;
}

#[tokio::test]
async fn emptied_topic_starts_over() {
    let url = spawn_server().await;
    let (mut a, a_id) = connect(&url).await;
    let (mut b, _) = connect(&url).await;

    subscribe(&mut b, "k").await;
    unsubscribe(&mut b, "k").await;
    publish(&mut a, "k", r#"{"seq":1}"#).await;
    subscribe(&mut b, "k").await;
    publish(&mut a, "k", r#"{"seq":2}"#).await;
    assert_eq!(
        next_json(&mut b).await,
        message_json("k", &a_id, json!({"seq": 2}))
    );
    assert_silent(&mut b).await;

    drop(b);
    tokio::time::sleep(Duration::from_millis(100)).await;
    publish(&mut a, "k", r#"{"seq":3}"#).await;
    let (mut c, _) = connect(&url).await;
    subscribe(&mut c, "k").await;
    publish(&mut a, "k", r#"{"seq":4}"#).await;
    assert_eq!(
        next_json(&mut c).await,
        message_json("k", &a_id, json!({"seq": 4}))
    );
    assert_silent(&mut c).await;
}

#[tokio::test]
async fn any_connection_can_join_any_topic() {
    let url = spawn_server().await;
    let (mut a, a_id) = connect(&url).await;
    let (mut b, _) = connect(&url).await;
    let (mut c, _) = connect(&url).await;
    subscribe(&mut b, "k").await;
    subscribe(&mut c, "k").await;

    publish(&mut a, "k", "1").await;
    assert_eq!(next_json(&mut b).await, message_json("k", &a_id, json!(1)));
    assert_eq!(next_json(&mut c).await, message_json("k", &a_id, json!(1)));

    unsubscribe(&mut b, "k").await;
    publish(&mut a, "k", "2").await;
    assert_eq!(next_json(&mut c).await, message_json("k", &a_id, json!(2)));
    assert_silent(&mut b).await;
}

#[tokio::test]
async fn topic_length_limits() {
    let url = spawn_server().await;
    let (mut a, _) = connect(&url).await;

    let longest = "x".repeat(255);
    subscribe(&mut a, &longest).await;

    subscribe_request(&mut a, "").await;
    let error = next_error(&mut a).await;
    assert_eq!(error["topic"], "");

    let too_long = "x".repeat(256);
    subscribe_request(&mut a, &too_long).await;
    let error = next_error(&mut a).await;
    assert_eq!(error["topic"], too_long.as_str());

    publish(&mut a, &too_long, "1").await;
    assert_eq!(next_error(&mut a).await["topic"], too_long.as_str());

    send(
        &mut a,
        WsMessage::text(json!({"type": "unsubscribe", "topic": ""}).to_string()),
    )
    .await;
    assert_eq!(next_error(&mut a).await["topic"], "");

    subscribe(&mut a, "still-works").await;
}

#[tokio::test]
async fn topics_compare_byte_for_byte() {
    let url = spawn_server().await;
    let (mut a, a_id) = connect(&url).await;
    let (mut b, _) = connect(&url).await;

    send(
        &mut b,
        WsMessage::text(r#"{"type":"subscribe","topic":"\u00e9"}"#),
    )
    .await;
    assert_eq!(
        next_json(&mut b).await,
        json!({"type": "subscribed", "topic": "é"})
    );

    send(
        &mut a,
        WsMessage::text(r#"{"type":"publish","topic":"é","data":1}"#),
    )
    .await;
    assert_eq!(next_json(&mut b).await, message_json("é", &a_id, json!(1)));

    publish(&mut a, "É", "2").await;
    publish(&mut a, "e", "3").await;
    assert_silent(&mut b).await;
}

#[tokio::test]
async fn subscription_limit_is_enforced() {
    let url = spawn_server().await;
    let (mut a, a_id) = connect(&url).await;
    let (mut b, _) = connect(&url).await;

    for i in 0..64 {
        subscribe(&mut b, &format!("t{i}")).await;
    }
    subscribe(&mut b, "t0").await;
    subscribe_request(&mut b, "t65").await;
    assert_eq!(next_error(&mut b).await["topic"], "t65");

    publish(&mut a, "t65", "1").await;
    publish(&mut a, "t0", r#""marker""#).await;
    assert_eq!(
        next_json(&mut b).await,
        message_json("t0", &a_id, json!("marker"))
    );
    assert_silent(&mut b).await;

    unsubscribe(&mut b, "t1").await;
    subscribe(&mut b, "t65").await;
}

#[tokio::test]
async fn errors_name_the_topic() {
    let url = spawn_server().await;
    let (mut a, _) = connect(&url).await;

    for i in 0..63 {
        subscribe(&mut a, &format!("t{i}")).await;
    }
    subscribe_request(&mut a, "a").await;
    subscribe_request(&mut a, "b").await;
    assert_eq!(
        next_json(&mut a).await,
        json!({"type": "subscribed", "topic": "a"})
    );
    assert_eq!(next_error(&mut a).await["topic"], "b");

    send(
        &mut a,
        WsMessage::text(json!({"type": "publish", "topic": "k"}).to_string()),
    )
    .await;
    assert_eq!(next_error(&mut a).await["topic"], "k");

    send(
        &mut a,
        WsMessage::text(json!({"type": "join", "topic": "k"}).to_string()),
    )
    .await;
    assert_eq!(next_error(&mut a).await["topic"], "k");

    send(&mut a, WsMessage::binary(vec![0x00, 0xff])).await;
    assert_eq!(next_error(&mut a).await["topic"], "");

    send(
        &mut a,
        WsMessage::text(r#"{"type":"join","topic":"\u00e9"}"#),
    )
    .await;
    assert_eq!(next_error(&mut a).await["topic"], "é");
}

#[tokio::test]
async fn errors_without_a_readable_topic_are_null() {
    let url = spawn_server().await;
    let (mut a, _) = connect(&url).await;

    for text in [
        "not json at all",
        r#"{"hp":42}"#,
        r#"{"type":"subscribe","topic":5}"#,
        r#"{"type":"publish","topic":"k","topic":"j","data":1}"#,
    ] {
        send(&mut a, WsMessage::text(text)).await;
        let error = next_error(&mut a).await;
        assert_eq!(error["topic"], Value::Null, "{text}");
        assert!(error.as_object().unwrap().contains_key("topic"));
    }

    send(&mut a, WsMessage::binary(vec![0x05, 0x6b])).await;
    assert_eq!(next_error(&mut a).await["topic"], Value::Null);
}

#[tokio::test]
async fn membership_churn_does_not_disturb_a_steady_receiver() {
    let url = spawn_server().await;
    let (mut publisher, publisher_id) = connect(&url).await;
    let (mut steady, _) = connect(&url).await;
    subscribe(&mut steady, "hot").await;

    let stop = Arc::new(AtomicBool::new(false));
    let cycles = Arc::new(AtomicUsize::new(0));
    let mut churners = Vec::new();
    for _ in 0..8 {
        let url = url.clone();
        let stop = Arc::clone(&stop);
        let cycles = Arc::clone(&cycles);
        churners.push(tokio::spawn(async move {
            let (mut client, _) = connect(&url).await;
            while !stop.load(Ordering::Relaxed) {
                for (kind, expected) in
                    [("subscribe", "subscribed"), ("unsubscribe", "unsubscribed")]
                {
                    send(
                        &mut client,
                        WsMessage::text(json!({"type": kind, "topic": "hot"}).to_string()),
                    )
                    .await;
                    loop {
                        let msg = next_json(&mut client).await;
                        if msg["type"] == expected {
                            break;
                        }
                    }
                }
                cycles.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }

    let chunk = 50;
    for start in (0..2_000).step_by(chunk) {
        for i in start..start + chunk {
            publish(&mut publisher, "hot", &json!({ "seq": i }).to_string()).await;
        }
        for i in start..start + chunk {
            assert_eq!(
                next_json(&mut steady).await,
                message_json("hot", &publisher_id, json!({ "seq": i }))
            );
        }
    }

    stop.store(true, Ordering::Relaxed);
    for churner in churners {
        churner.await.unwrap();
    }
    assert!(cycles.load(Ordering::Relaxed) > 0, "no churn happened");

    let (mut late, _) = connect(&url).await;
    subscribe(&mut late, "hot").await;
    publish(&mut publisher, "hot", r#""after""#).await;
    assert_eq!(next_json(&mut late).await["data"], json!("after"));
    assert_eq!(next_json(&mut steady).await["data"], json!("after"));
}

type Refusal = Response<Option<Vec<u8>>>;

async fn connect_with(url: &str, headers: &[(&str, &str)]) -> Result<(Client, Refusal), WsError> {
    let mut request = url.into_client_request().unwrap();
    for (name, value) in headers {
        request.headers_mut().append(
            tokio_tungstenite::tungstenite::http::HeaderName::from_bytes(name.as_bytes()).unwrap(),
            value.parse().unwrap(),
        );
    }
    connect_async(request).await
}

async fn refused_with(url: &str, headers: &[(&str, &str)]) -> Refusal {
    match connect_with(url, headers).await {
        Err(WsError::Http(response)) => *response,
        Err(other) => panic!("expected an HTTP refusal, got {other:?}"),
        Ok(_) => panic!("expected a refusal, the upgrade succeeded"),
    }
}

type Comparable = (u16, Vec<(String, Vec<u8>)>, Vec<u8>);

fn comparable(response: &Refusal) -> Comparable {
    let mut headers: Vec<(String, Vec<u8>)> = response
        .headers()
        .iter()
        .filter(|(name, _)| name.as_str() != "date")
        .map(|(name, value)| (name.to_string(), value.as_bytes().to_vec()))
        .collect();
    headers.sort();
    (
        response.status().as_u16(),
        headers,
        response.body().clone().unwrap_or_default(),
    )
}

fn assert_plain_401(response: &Refusal) {
    assert_eq!(response.status().as_u16(), 401);
    assert_eq!(response.headers()["www-authenticate"], "Bearer");
    assert!(
        response.body().as_deref().unwrap_or_default().is_empty(),
        "refusal body must be empty"
    );
}

async fn welcomed(client: &mut Client) -> String {
    let welcome = next_json(client).await;
    assert_eq!(welcome["type"], "welcome");
    welcome["id"].as_str().unwrap().to_string()
}

#[derive(Clone, Default)]
struct LogCapture(Arc<std::sync::Mutex<Vec<u8>>>);

impl std::io::Write for LogCapture {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for LogCapture {
    type Writer = LogCapture;

    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

fn global_logs() -> &'static LogCapture {
    static LOGS: std::sync::OnceLock<LogCapture> = std::sync::OnceLock::new();
    LOGS.get_or_init(|| {
        let logs = LogCapture::default();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(logs.clone())
            .with_ansi(false)
            .with_max_level(tracing::Level::INFO)
            .finish();
        tracing::subscriber::set_global_default(subscriber).unwrap();
        logs
    })
}

impl LogCapture {
    fn text(&self) -> String {
        String::from_utf8(self.0.lock().unwrap().clone()).unwrap()
    }
}

async fn eventually_logged(logs: &LogCapture, needle: &str) {
    for _ in 0..100 {
        if logs.text().contains(needle) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("log never contained {needle:?}:\n{}", logs.text());
}

#[tokio::test]
async fn connection_is_admitted_without_credentials_when_auth_is_off() {
    let url = spawn_server_with_tokens("").await;
    let (mut client, _) = connect_async(&url).await.unwrap();
    welcomed(&mut client).await;
}

#[tokio::test]
async fn bearer_header_token_is_admitted() {
    let url = spawn_server_with_tokens("t1,t2").await;
    let (mut client, _) = connect_with(&url, &[("authorization", "Bearer t2")])
        .await
        .unwrap();
    welcomed(&mut client).await;
}

#[tokio::test]
async fn subprotocol_token_is_admitted() {
    let url = spawn_server_with_tokens("t1").await;
    let (mut client, response) = connect_with(&url, &[("sec-websocket-protocol", "bearer, t1")])
        .await
        .unwrap();
    let selected: Vec<_> = response
        .headers()
        .get_all("sec-websocket-protocol")
        .iter()
        .collect();
    assert_eq!(selected, ["bearer"]);
    welcomed(&mut client).await;
}

#[tokio::test]
async fn authorization_header_wins_over_subprotocol() {
    let url = spawn_server_with_tokens("t1").await;
    let response = refused_with(
        &url,
        &[
            ("authorization", "Bearer wrong"),
            ("sec-websocket-protocol", "bearer, t1"),
        ],
    )
    .await;
    assert_plain_401(&response);
}

#[tokio::test]
async fn missing_credential_is_refused_with_401() {
    let url = spawn_server_with_tokens("t1").await;
    let response = refused_with(&url, &[]).await;
    assert_plain_401(&response);
}

#[tokio::test]
async fn wrong_token_is_refused_with_401() {
    let url = spawn_server_with_tokens("t1,xt1").await;
    let missing = refused_with(&url, &[]).await;
    assert_plain_401(&missing);

    let wrong_cases: [&[(&str, &str)]; 9] = [
        &[("authorization", "Bearer t3")],
        &[("authorization", "Basic t1")],
        &[("authorization", "Bearerxt1")],
        &[
            ("authorization", "Basic x"),
            ("sec-websocket-protocol", "bearer, t1"),
        ],
        &[("authorization", "Basic xyz")],
        &[("authorization", "Bearer")],
        &[("authorization", "Bearer ")],
        &[("sec-websocket-protocol", "bearer, t3")],
        &[("sec-websocket-protocol", "t1")],
    ];
    for headers in wrong_cases {
        let response = refused_with(&url, headers).await;
        assert_plain_401(&response);
        assert_eq!(comparable(&response), comparable(&missing), "{headers:?}");
    }
}

#[tokio::test]
async fn query_string_token_is_not_accepted() {
    let url = spawn_server_with_tokens("t1").await;
    let response = refused_with(&format!("{url}?token=t1"), &[]).await;
    assert_plain_401(&response);
}

#[tokio::test]
async fn refused_requests_do_not_count_as_connections() {
    let logs = global_logs();

    let url = spawn_server_with_tokens("t1").await;
    for _ in 0..3 {
        refused_with(&url, &[("authorization", "Bearer secret-guess")]).await;
    }
    refused_with(&url, &[("sec-websocket-protocol", "bearer, secret-guess2")]).await;
    let (mut first, _) = connect_with(&url, &[("authorization", "Bearer t1")])
        .await
        .unwrap();
    let first_id = welcomed(&mut first).await;
    eventually_logged(logs, &format!("{first_id} connected (total: 1)")).await;
    let (mut second, _) = connect_with(&url, &[("authorization", "Bearer t1")])
        .await
        .unwrap();
    let second_id = welcomed(&mut second).await;
    eventually_logged(logs, &format!("{second_id} connected (total: 2)")).await;

    let text = logs.text();
    assert!(!text.contains("secret-guess"), "{text}");
    assert!(!text.contains("secret-guess2"), "{text}");
}

#[tokio::test]
async fn each_refusal_logs_one_warn_with_the_peer_address() {
    let logs = global_logs();
    let url = spawn_server_with_tokens("t1").await;
    let host = url.trim_start_matches("ws://").trim_end_matches("/ws");

    let mut peers = Vec::new();
    for guess in ["peer-guess-a", "peer-guess-b"] {
        let stream = tokio::net::TcpStream::connect(host).await.unwrap();
        peers.push(stream.local_addr().unwrap());
        let mut request = url.as_str().into_client_request().unwrap();
        request
            .headers_mut()
            .insert("authorization", format!("Bearer {guess}").parse().unwrap());
        let refusal = tokio_tungstenite::client_async(request, stream).await;
        assert!(matches!(refusal, Err(WsError::Http(_))));
    }

    for peer in peers {
        let needle = format!("{peer} refused");
        eventually_logged(logs, &needle).await;
        let text = logs.text();
        let lines: Vec<_> = text.lines().filter(|l| l.contains(&needle)).collect();
        assert_eq!(lines.len(), 1, "{text}");
        assert!(!lines[0].contains("peer-guess"), "{}", lines[0]);
    }
}

#[tokio::test]
async fn authenticated_connection_is_welcomed_with_an_id() {
    let url = spawn_server_with_tokens("t1").await;
    let (mut client, _) = connect_with(&url, &[("authorization", "Bearer t1")])
        .await
        .unwrap();
    let id = welcomed(&mut client).await;
    assert!(uuid::Uuid::parse_str(&id).is_ok(), "not a uuid: {id}");
    assert!(!id.contains("t1"));
}

#[tokio::test]
async fn authenticated_connections_relay_to_each_other() {
    let url = spawn_server_with_tokens("t1,t2").await;
    let (mut a, _) = connect_with(&url, &[("authorization", "Bearer t1")])
        .await
        .unwrap();
    let a_id = welcomed(&mut a).await;
    let (mut b, _) = connect_with(&url, &[("sec-websocket-protocol", "bearer, t2")])
        .await
        .unwrap();
    welcomed(&mut b).await;
    subscribe(&mut b, "k").await;
    subscribe(&mut a, "k").await;

    publish(&mut a, "k", r#"{"n":1}"#).await;

    assert_eq!(
        next_json(&mut b).await,
        message_json("k", &a_id, json!({"n": 1}))
    );
}

#[tokio::test]
async fn credentials_are_never_an_in_band_frame() {
    let url = spawn_server_with_tokens("t1").await;
    let (mut client, _) = connect_with(&url, &[("authorization", "Bearer t1")])
        .await
        .unwrap();
    welcomed(&mut client).await;

    send(
        &mut client,
        WsMessage::text(r#"{"type":"auth","token":"t1"}"#),
    )
    .await;
    next_error(&mut client).await;

    send(&mut client, WsMessage::text("t1")).await;
    next_error(&mut client).await;
}

struct Gateway {
    child: std::process::Child,
    output: std::sync::mpsc::Receiver<String>,
}

impl Drop for Gateway {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn start_gateway(tokens: &std::ffi::OsStr) -> Gateway {
    use std::io::{BufRead, BufReader};
    use std::process::{Command, Stdio};

    let mut child = Command::new(env!("CARGO_BIN_EXE_realtime-gateway"))
        .env("GATEWAY_ADDR", "127.0.0.1:0")
        .env("GATEWAY_AUTH_TOKENS", tokens)
        .env("RUST_LOG", "realtime_gateway=info")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let (sender, output) = std::sync::mpsc::channel();
    let stdout = child.stdout.take().unwrap();
    let stderr = child.stderr.take().unwrap();
    let from_stdout = sender.clone();
    std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines().map_while(Result::ok) {
            let _ = from_stdout.send(line);
        }
    });
    std::thread::spawn(move || {
        for line in BufReader::new(stderr).lines().map_while(Result::ok) {
            let _ = sender.send(line);
        }
    });
    Gateway { child, output }
}

impl Gateway {
    fn wait_for_exit(&mut self) -> std::process::ExitStatus {
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        loop {
            if let Some(status) = self.child.try_wait().unwrap() {
                return status;
            }
            assert!(std::time::Instant::now() < deadline, "gateway did not exit");
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    fn drain_output(&self) -> String {
        let mut text = String::new();
        while let Ok(line) = self.output.recv_timeout(Duration::from_millis(500)) {
            text.push_str(&line);
            text.push('\n');
        }
        text
    }

    fn wait_for_line(&self, needle: &str) -> String {
        let mut seen = String::new();
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while std::time::Instant::now() < deadline {
            if let Ok(line) = self.output.recv_timeout(Duration::from_millis(100)) {
                seen.push_str(&line);
                seen.push('\n');
                if line.contains(needle) {
                    return seen;
                }
            }
        }
        panic!("output never contained {needle:?}:\n{seen}");
    }
}

#[test]
fn invalid_token_entry_stops_startup_naming_only_its_position() {
    let mut gateway = start_gateway("ok,a b".as_ref());
    let status = gateway.wait_for_exit();
    let output = gateway.drain_output();
    assert_eq!(status.code(), Some(1), "{output}");
    assert!(output.contains("position 2"), "{output}");
    assert!(!output.contains("a b"), "{output}");
    assert!(!output.contains("authentication is off"), "{output}");
}

#[test]
fn empty_token_configuration_logs_that_authentication_is_off() {
    let gateway = start_gateway("".as_ref());
    gateway.wait_for_line("authentication is off: any client can connect");
}

#[test]
fn configured_tokens_do_not_log_that_authentication_is_off() {
    let gateway = start_gateway("t1".as_ref());
    let seen = gateway.wait_for_line("gateway listening on");
    assert!(!seen.contains("authentication is off"), "{seen}");
    assert!(!seen.contains("t1"), "{seen}");
}

#[cfg(unix)]
#[test]
fn non_utf8_token_configuration_stops_startup() {
    use std::os::unix::ffi::OsStrExt;

    let mut gateway = start_gateway(std::ffi::OsStr::from_bytes(b"\xff"));
    let status = gateway.wait_for_exit();
    let output = gateway.drain_output();
    assert_eq!(status.code(), Some(1), "{output}");
    assert!(!output.contains("authentication is off"), "{output}");
}
