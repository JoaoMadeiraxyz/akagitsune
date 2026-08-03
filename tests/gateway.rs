use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async, tungstenite::Message as WsMessage,
};

type Client = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

async fn spawn_server() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(realtime_gateway::run(listener, std::future::pending()));
    format!("ws://{addr}/ws")
}

async fn next_msg(client: &mut Client) -> WsMessage {
    tokio::time::timeout(Duration::from_secs(5), client.next())
        .await
        .expect("timed out waiting for a message")
        .expect("stream closed")
        .unwrap()
}

async fn next_json(client: &mut Client) -> Value {
    serde_json::from_str(&next_msg(client).await.into_text().unwrap()).unwrap()
}

async fn assert_silent(client: &mut Client) {
    let unexpected = tokio::time::timeout(Duration::from_millis(200), client.next()).await;
    assert!(unexpected.is_err(), "unexpected frame: {unexpected:?}");
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
async fn payload_is_relayed_verbatim_to_others() {
    let url = spawn_server().await;
    let (mut a, a_id) = connect(&url).await;
    let (mut b, _) = connect(&url).await;

    let payload = json!({"hp": 42, "pos": [1, 2], "nested": {"any": null}});
    a.send(WsMessage::text(payload.to_string())).await.unwrap();

    assert_eq!(
        next_json(&mut b).await,
        json!({"type": "message", "from": a_id, "data": payload})
    );
    assert_silent(&mut a).await;
}

#[tokio::test]
async fn any_json_shape_is_accepted() {
    let url = spawn_server().await;
    let (mut a, _) = connect(&url).await;
    let (mut b, _) = connect(&url).await;

    for payload in [json!(42), json!("texto"), json!([1, 2, 3]), json!(null)] {
        a.send(WsMessage::text(payload.to_string())).await.unwrap();
        assert_eq!(next_json(&mut b).await["data"], payload);
    }
}

#[tokio::test]
async fn binary_frames_pass_through_untouched() {
    let url = spawn_server().await;
    let (mut a, _) = connect(&url).await;
    let (mut b, _) = connect(&url).await;

    let bytes = vec![0x00, 0xff, 0x10, 0x42];
    a.send(WsMessage::binary(bytes.clone())).await.unwrap();

    assert_eq!(next_msg(&mut b).await, WsMessage::binary(bytes));
    assert_silent(&mut a).await;
}

#[tokio::test]
async fn invalid_json_is_rejected_without_broadcasting() {
    let url = spawn_server().await;
    let (mut a, _) = connect(&url).await;
    let (mut b, _) = connect(&url).await;

    a.send(WsMessage::text("not json at all")).await.unwrap();

    assert_eq!(next_json(&mut a).await["type"], "error");
    assert_silent(&mut b).await;

    a.send(WsMessage::text(json!({"ok": true}).to_string()))
        .await
        .unwrap();
    assert_eq!(next_json(&mut b).await["data"], json!({"ok": true}));
}
