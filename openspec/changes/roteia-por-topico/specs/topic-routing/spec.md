# Spec Delta

## ADDED Requirements

### Requirement: Subscribing to a topic is acknowledged
A connection SHALL subscribe to a topic by sending the text frame `{"type":"subscribe","topic":"<key>"}`. The gateway SHALL answer with `{"type":"subscribed","topic":"<key>"}` once the subscription is in effect. Subscribing to a topic the connection is already subscribed to SHALL be answered the same way and SHALL NOT create a second subscription.

Fonte: planned — `src/registry.rs` — `TopicRegistry`; `src/ws.rs` — `handle_socket`; `src/protocol.rs` — `ClientFrame`.
Teste: planned — `tests/gateway.rs` — `subscribe_is_acknowledged`; `tests/gateway.rs` — `duplicate_subscribe_delivers_once`.

#### Scenario: Subscribe is acknowledged
- **WHEN** a connection sends `{"type":"subscribe","topic":"k"}`
- **THEN** it receives `{"type":"subscribed","topic":"k"}`

#### Scenario: Duplicate subscribe delivers once
- **WHEN** connection B sends `{"type":"subscribe","topic":"k"}` twice and receives both acknowledgements
- **AND** connection A sends `{"type":"publish","topic":"k","data":1}`
- **THEN** B receives exactly one `message` frame for that publish

### Requirement: A publish reaches only the other subscribers of its topic
A connection SHALL publish by sending `{"type":"publish","topic":"<key>","data":<payload>}`. The gateway SHALL deliver it to every other connection subscribed to `<key>` and to no connection that is not. Publishing SHALL NOT require the publisher to be subscribed. A publish to a topic with no subscribers SHALL be discarded without any frame to the publisher.

Fonte: planned — `src/ws.rs` — `handle_socket`; `src/registry.rs` — `TopicRegistry`.
Teste: planned — `tests/gateway.rs` — `publish_reaches_only_subscribers_of_its_topic`; `tests/gateway.rs` — `publisher_need_not_be_subscribed`; `tests/gateway.rs` — `publish_to_empty_topic_is_silent`.

#### Scenario: Only subscribers of the topic receive
- **WHEN** connection B is subscribed to `k`, connection C is subscribed to `other`, and connection D has no subscriptions
- **AND** connection A sends `{"type":"publish","topic":"k","data":{"x":1}}`
- **THEN** B receives `{"type":"message","topic":"k","from":"<A's id>","data":{"x":1}}`
- **AND** C and D receive nothing

#### Scenario: Publisher need not be subscribed
- **WHEN** connection B is subscribed to `k` and connection A has no subscriptions
- **AND** A publishes to `k`
- **THEN** B receives the `message` frame

#### Scenario: Publish to a topic nobody subscribes to is silent
- **WHEN** connection A publishes to a topic no connection is subscribed to
- **THEN** A receives nothing for it
- **AND** A's connection stays open

### Requirement: A subscription is visible to every publish made after its acknowledgement
Once a connection has received `subscribed` for a topic, every publish to that topic sent by another connection after that point SHALL be delivered to it, subject to backpressure. A frame for the topic MAY arrive before `subscribed` when the publish raced the subscription.

Fonte: planned — `src/registry.rs` — `TopicRegistry`; `src/ws.rs` — `handle_socket`.
Teste: planned — `tests/gateway.rs` — `publish_after_subscribed_is_delivered`.

#### Scenario: Publish after acknowledgement is delivered
- **WHEN** connection B sends `subscribe` for `k` and receives `subscribed`
- **AND** connection A then publishes to `k`
- **THEN** B receives the `message` frame

### Requirement: Unsubscribing stops delivery
A connection SHALL unsubscribe by sending `{"type":"unsubscribe","topic":"<key>"}`. The gateway SHALL answer with `{"type":"unsubscribed","topic":"<key>"}`, and SHALL NOT deliver to it any publish to `<key>` sent after that acknowledgement was received. Frames for `<key>` already being delivered when the unsubscribe was processed MAY still arrive after the acknowledgement. Unsubscribing from a topic the connection is not subscribed to SHALL be answered the same way.

Fonte: planned — `src/registry.rs` — `TopicRegistry`; `src/ws.rs` — `handle_socket`.
Teste: planned — `tests/gateway.rs` — `unsubscribe_stops_delivery`; `tests/gateway.rs` — `unsubscribe_without_subscription_is_acknowledged`.

#### Scenario: No delivery after unsubscribe
- **WHEN** connection B subscribes to `k`, then sends `unsubscribe` for `k` and receives `{"type":"unsubscribed","topic":"k"}`
- **AND** connection A then publishes to `k`, and afterwards publishes a marker to a topic `probe` that B is subscribed to
- **THEN** B receives the marker and no `message` frame for `k` sent after the acknowledgement

#### Scenario: Unsubscribe without a subscription is acknowledged
- **WHEN** a connection with no subscriptions sends `{"type":"unsubscribe","topic":"k"}`
- **THEN** it receives `{"type":"unsubscribed","topic":"k"}`

### Requirement: A disconnected connection is removed from every topic
When a connection ends for any reason, the gateway SHALL remove it from every topic it was subscribed to, and later publishes SHALL be delivered to the remaining subscribers as if it had never subscribed.

Fonte: planned — `src/registry.rs` — `Subscriptions`.
Teste: planned — `tests/gateway.rs` — `disconnect_leaves_other_subscribers_working`.

#### Scenario: Remaining subscribers keep receiving
- **WHEN** connections B and C are subscribed to `k` and B closes its socket
- **AND** connection A then publishes 100 frames to `k`
- **THEN** C receives all 100 in order

### Requirement: A topic exists only while it has subscribers
A topic SHALL come into existence when the first connection subscribes to its key, and SHALL cease to exist when its last subscriber leaves by `unsubscribe` or disconnection. A `publish` SHALL NOT create a topic. The gateway SHALL NOT retain any frame, member or setting for a topic across a period in which it had no subscribers, and a frame published during such a period SHALL NOT be delivered to any connection that subscribes later.

Fonte: planned — `src/registry.rs` — `TopicRegistry`; `src/registry.rs` — `Subscriptions`.
Teste: planned — `tests/gateway.rs` — `publish_before_any_subscription_is_not_retained`; `tests/gateway.rs` — `emptied_topic_starts_over`.

#### Scenario: Publish before any subscription is not retained
- **WHEN** connection A publishes `{"seq":1}` to `k` while no connection is subscribed to `k`
- **AND** connection B then subscribes to `k` and receives `subscribed`
- **AND** A then publishes `{"seq":2}` to `k`
- **THEN** B receives only the frame with `{"seq":2}`

#### Scenario: Emptied topic starts over
- **WHEN** connection B subscribes to `k`, then unsubscribes and receives `unsubscribed`, leaving `k` without subscribers
- **AND** connection A publishes `{"seq":1}` to `k`
- **AND** B subscribes to `k` again and receives `subscribed`, and A then publishes `{"seq":2}`
- **THEN** B receives only the frame with `{"seq":2}`

#### Scenario: Topic emptied by disconnection starts over
- **WHEN** connection B, the only subscriber of `k`, closes its socket
- **AND** connection A publishes `{"seq":1}` to `k`
- **AND** a new connection C subscribes to `k` and receives `subscribed`, and A then publishes `{"seq":2}`
- **THEN** C receives only the frame with `{"seq":2}`

### Requirement: Topics have no owner and no access control
Any connection SHALL be able to subscribe and publish to any valid topic key, regardless of which connection subscribed to it first. No connection SHALL be able to close a topic or remove other connections from it. Connections that choose the same key SHALL share the same topic.

Fonte: planned — `src/ws.rs` — `handle_socket`; `src/registry.rs` — `TopicRegistry`.
Teste: planned — `tests/gateway.rs` — `any_connection_can_join_any_topic`.

#### Scenario: Unrelated connections share a key
- **WHEN** connection B subscribes to `k`
- **AND** an unrelated connection C subscribes to `k`
- **AND** connection A publishes to `k`
- **THEN** both B and C receive the `message` frame

#### Scenario: Topic outlives its first subscriber
- **WHEN** connection B subscribes to `k` first, and connection C subscribes to `k` afterwards
- **AND** B unsubscribes from `k`
- **AND** connection A publishes to `k`
- **THEN** C still receives the `message` frame

### Requirement: Topics are 1 to 255 bytes of UTF-8
The gateway SHALL accept as a topic any string of 1 to 255 bytes of UTF-8, measured after JSON unescaping, and SHALL compare topics byte for byte. A `subscribe`, `unsubscribe` or `publish` whose topic is empty or longer than 255 bytes SHALL be answered with `{"type":"error","topic":"<the topic as sent>","message":"<INVALID_TOPIC>"}`, SHALL have no other effect, and SHALL leave the connection open.

Fonte: planned — `src/protocol.rs` — `MAX_TOPIC_LEN`; `src/ws.rs` — `INVALID_TOPIC`.
Teste: planned — `tests/gateway.rs` — `topic_length_limits`; `tests/gateway.rs` — `topics_compare_byte_for_byte`.

#### Scenario: Topic at 255 bytes is accepted
- **WHEN** a connection subscribes to a topic of exactly 255 bytes
- **THEN** it receives `subscribed` for that topic

#### Scenario: Empty or oversized topic is rejected
- **WHEN** a connection sends `subscribe` with topic `""`, then with a topic of 256 bytes
- **THEN** it receives an `error` frame with `"topic":""` for the first and with the 256-byte topic for the second
- **AND** a later valid frame from it is still processed

#### Scenario: Escaped and literal forms are the same topic
- **WHEN** connection B subscribes with the topic written as the JSON escape `"\u00e9"`
- **AND** connection A publishes with the topic written as the literal character `"é"`
- **THEN** B receives the `message` frame

### Requirement: A connection holds at most 64 subscriptions
A connection SHALL be able to hold up to 64 distinct topic subscriptions. A `subscribe` to a new topic beyond that SHALL be answered with `{"type":"error","topic":"<that topic>","message":"<SUBSCRIPTION_LIMIT>"}` and SHALL NOT change the connection's subscriptions.

Fonte: planned — `src/registry.rs` — `MAX_SUBSCRIPTIONS_PER_CONNECTION`; `src/ws.rs` — `SUBSCRIPTION_LIMIT`.
Teste: planned — `tests/gateway.rs` — `subscription_limit_is_enforced`.

#### Scenario: Sixty-fifth topic is rejected
- **WHEN** a connection subscribes to 64 distinct topics and receives 64 acknowledgements
- **AND** it subscribes to a 65th topic `t65`
- **THEN** it receives an `error` frame with `"topic":"t65"`
- **AND** a publish by another connection to the 65th topic does not reach it
- **AND** after unsubscribing from one topic, subscribing to the 65th is acknowledged

### Requirement: Every error frame names the topic it concerns
Every `error` frame SHALL have the shape `{"type":"error","topic":<topic or null>,"message":"<text>"}`, with the `topic` field always present. When the frame that caused the error was parsed as a control frame (whatever its `type`), or was a binary frame whose length byte and topic bytes could be read, `topic` SHALL be the topic exactly as the client sent it, after JSON unescaping, even when that topic is itself invalid. When no topic could be read, because the frame was not JSON, had no string `topic` field, or was a binary frame too short for its declared topic or with a topic that is not UTF-8, `topic` SHALL be `null`.

Fonte: planned — `src/protocol.rs` — `ServerMessage`; `src/ws.rs` — `handle_socket`.
Teste: planned — `tests/gateway.rs` — `errors_name_the_topic`; `tests/gateway.rs` — `errors_without_a_readable_topic_are_null`.

#### Scenario: Several subscribes, one rejected
- **WHEN** a connection holding 63 subscriptions sends `subscribe` for `a` and then for `b` without waiting
- **THEN** it receives `{"type":"subscribed","topic":"a"}`
- **AND** an `error` frame with `"topic":"b"`

#### Scenario: Publish without data names its topic
- **WHEN** a connection sends `{"type":"publish","topic":"k"}`
- **THEN** it receives an `error` frame with `"topic":"k"`

#### Scenario: Unknown type names its topic
- **WHEN** a connection sends `{"type":"join","topic":"k"}`
- **THEN** it receives an `error` frame with `"topic":"k"`

#### Scenario: No readable topic gives null
- **WHEN** a connection sends the text frame `not json at all`, then `{"hp":42}`, then `{"type":"subscribe","topic":5}`
- **THEN** it receives three `error` frames, each with `"topic":null`

#### Scenario: Zero-length binary topic gives an empty topic
- **WHEN** a connection sends a binary frame with bytes `00 ff`
- **THEN** it receives an `error` frame with `"topic":""`

#### Scenario: Truncated binary header gives null
- **WHEN** a connection sends a binary frame with bytes `05 6b`
- **THEN** it receives an `error` frame with `"topic":null`
