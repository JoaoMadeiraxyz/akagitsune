# Verificacao de testa-limite-de-tamanho-de-frame

Requisito sem citacao conta como NAO coberto -- nao como provavelmente coberto.

### Frames above 64 KiB terminate the connection

- prova (Frame at the limit is relayed): `tests/gateway.rs:62` envia `json_string_of_len(64 * 1024)` (`tests/gateway.rs:36-38`, exatamente 65536 bytes de JSON valido: aspas + 65534 `x` + aspas); `tests/gateway.rs:65-68` -- `assert_eq!(next_json(&mut b).await, json!({"type": "message", "from": a_id, "data": data}))`, com `data` reparseado do proprio payload enviado (`tests/gateway.rs:64`), nao de constante do codigo sob teste. A igualdade e do envelope inteiro (tipo, remetente e `data`).
- prova (Oversized frame drops the sender -- conexao do remetente termina sem close frame): `tests/gateway.rs:78` envia `json_string_of_len(64 * 1024 + 1)` (65537 bytes); `tests/gateway.rs:82-89` -- `tokio::time::timeout(Duration::from_secs(5), a.next()).await.expect("sender connection is still open")` exige que o stream do remetente produza algo em 5 s, e o `match` so aceita `None | Some(Err(_))`; `Some(Ok(WsMessage::Close(frame)))` faz `panic!("unexpected close frame: {frame:?}")` e qualquer outro frame faz `panic!("unexpected frame: {other:?}")`.
- prova (Oversized frame drops the sender -- nenhuma outra conexao recebe nada): `tests/gateway.rs:90` -- `assert_silent(&mut b).await`, que em `tests/gateway.rs:31-34` faz `assert!(unexpected.is_err(), ...)` sobre `timeout(200ms, client.next())`. Roda depois de a queda do remetente ter sido observada, entao o reader do remetente ja terminou e nao ha relay pendente.

Observacoes (nao bloqueiam):

- lacuna de precisao da spec (`Fonte:`): `src/ws.rs:96` -- `handle_socket` aponta para o loop do reader; `handle_socket` e declarado em `src/ws.rs:37`. A linha ja vinha assim da spec consolidada (`openspec/specs/connection-lifecycle/spec.md:25`) e esta change nao a alterou. Corrigir para `src/ws.rs:37` numa proxima edicao da spec.
- A assercao de silencio do receptor nao foi discriminada isoladamente por mutante: nos mutantes que relayam o frame grande (M1, M3), o teste morre antes, na linha 83, porque o remetente nao cai. Nao ha mutacao simples em `src/` que derrube o remetente e ainda assim relaye, porque o limite e aplicado pelo axum antes do reader.

## Sensor de discriminacao

Linha de base `git status --porcelain` da worktree real: vazia antes e vazia depois (iguais). Cada mutacao rodou numa `git worktree add --detach /tmp/vf-mN HEAD` propria, removida com `git worktree remove --force` ao final. Suite: `cargo test --test gateway`.

- mutacao M1 (tasks.md 3.3): `src/ws.rs:20`, `MAX_MESSAGE_SIZE` de `64 * 1024` para `64 * 1024 + 1`
- resultado: o teste morreu -- `oversized_frame_drops_the_sender_without_relaying` FAILED, panic em `tests/gateway.rs:83` ("sender connection is still open": o remetente nao caiu em 5 s). Demais 8 testes passaram.

- mutacao M2: `src/ws.rs:20`, `MAX_MESSAGE_SIZE` de `64 * 1024` para `64 * 1024 - 1` (off-by-one para baixo)
- resultado: o teste morreu -- `frame_at_size_limit_is_relayed` FAILED, panic em `tests/gateway.rs:22` ("timed out waiting for a message": B nao recebeu o frame de 65536 bytes). Demais 8 testes passaram.

- mutacao M3: `src/ws.rs:28`, remover a chamada `.max_message_size(MAX_MESSAGE_SIZE)` (volta ao default do axum, 64 MiB)
- resultado: o teste morreu -- `oversized_frame_drops_the_sender_without_relaying` FAILED, panic em `tests/gateway.rs:83`. Demais 8 testes passaram.

Os tres mutantes compilaram; nenhum inconclusivo.

## Quem rodou, e quando

- comando: cargo fmt --check && cargo clippy --all-targets -- -D warnings && cargo test
- resultado: pass (fmt limpo, clippy sem warnings, todos os testes verdes) em HEAD `0a0c041`, 2026-10-05
- testes antes/depois: 14 em main `0edcbf3` (7 em `tests/gateway.rs` + 7 unitarios de `examples/loadgen.rs`) / 16 em HEAD (9 + 7); os dois novos sao `frame_at_size_limit_is_relayed` e `oversized_frame_drops_the_sender_without_relaying`
- validacao: `openspec validate testa-limite-de-tamanho-de-frame --strict` valido; validacao de regras das changes ok
- escopo: o diff `main...HEAD` toca apenas `tests/gateway.rs`, `README.md` (paragrafo Limits), o delta `specs/connection-lifecycle/spec.md` (linha `Teste:`) e `tasks.md`. Nenhuma mudanca em `src/`, conforme a proposta. A spec consolidada sera atualizada no archive.
- CI: .github/workflows/ci.yml roda os mesmos tres comandos no PR; main nao tem branch protection, entao o revisor precisa confirmar o run verde antes do merge
- sessao independente: subagente verificador em sessao nova, sem o historico da implementacao

## Veredito

aprovado

Os dois cenarios tem prova `arquivo:linha` com assercao exata, os tres mutantes validos morreram, a suite cresceu de 14 para 16 e o diff fica dentro do escopo da proposta. Ficam duas observacoes nao bloqueantes, descritas acima: a linha `Fonte:` aponta `handle_socket` na linha 96 em vez da 37, e a assercao de silencio do receptor nao foi discriminada isoladamente.
