# Verificacao de limita-tempo-de-envio-nos-testes

Change sem requisitos (skip_specs). Cada objetivo da proposta precisa de prova arquivo:linha.

### Helper de envio com timeout de 5 s e mensagem clara
- prova: `tests/gateway.rs:27` -- `async fn send(client: &mut Client, msg: WsMessage)` envolve `client.send(msg)` em `tokio::time::timeout(Duration::from_secs(5), ..)` (`tests/gateway.rs:28`), no mesmo formato de `next_msg` (`tests/gateway.rs:19`).
- prova: `tests/gateway.rs:30` -- `.expect("timed out sending a frame")`, a mensagem exata do design.md. No sensor, os testes travados falharam com `panicked at tests/gateway.rs:30:10: timed out sending a frame: Elapsed(())`.

### Todo envio de tests/gateway.rs passa pelo helper
- prova: `rg '\.send\(' tests/gateway.rs` casa apenas `tests/gateway.rs:28`, dentro do proprio helper.
- prova: as 15 chamadas `.send(..).await.unwrap()` de `main` viraram 15 chamadas do helper: `tests/gateway.rs:69`, `:84`, `:104`, `:120`, `:126`, `:140`, `:152`, `:166`, `:182`, `:201`, `:203`, `:245`, `:252`, `:278`, `:283`. Isso inclui os tres lacos de rajada (`:182`, `:201`, `:245`).

### Nenhuma asercao mudou e nenhum teste foi removido
- prova: em `git diff main...HEAD -- tests/gateway.rs`, todos os hunks trocam so o formato da chamada de envio (`a.send(X).await.unwrap()` -> `send(&mut a, X).await`), mais a adicao do helper. Nenhuma linha com `assert`, `next_msg`, `next_json`, `assert_silent` ou `match` foi tocada; a contagem de linhas com `assert` e a mesma em main e HEAD.
- prova: os nomes `fn` em main e HEAD sao iguais, exceto pelo `fn send` novo; os mesmos 12 testes `#[tokio::test]` (`tests/gateway.rs:55` a `tests/gateway.rs:272`).

### Escopo
- prova: `git diff --stat main...HEAD` toca apenas `tests/gateway.rs` e `openspec/changes/limita-tempo-de-envio-nos-testes/tasks.md`. Nada em `src/` nem em `.github/`.

## Sensor de discriminacao
- mutacao: em worktrees isoladas (destacadas de HEAD e de main), inserida antes de `tx_global.send(BroadcastMessage {` em `src/ws.rs` a linha `while tx_global.len() >= crate::state::BROADCAST_CAPACITY - 1 { tokio::time::sleep(std::time::Duration::from_millis(1)).await; }` (publicar passa a esperar quando o bus esta quase cheio). Cada worktree compilou num target dir proprio e novo; o cargo mostrou `Compiling realtime-gateway` a partir do caminho da worktree mutada, entao nenhum binario antigo foi reaproveitado. Cada teste rodou sozinho (`<binario> <teste> --exact`) com alarme de 60 s; codigo de saida 142 = morto pelo alarme = travou.
- resultado em HEAD (nenhum travou, todos terminaram em menos de 30 s):
  - connection_is_welcomed_with_an_id: passou
  - frame_at_size_limit_is_relayed: passou
  - oversized_frame_drops_the_sender_without_relaying: passou
  - payload_is_relayed_verbatim_to_others: passou
  - text_payload_bytes_are_relayed_verbatim: passou
  - any_json_shape_is_accepted: passou
  - binary_frames_pass_through_untouched: passou
  - fifo_order_holds_across_a_multi_batch_burst: passou
  - slow_consumer_receives_a_warning_frame: falhou em 5.2 s (`timed out sending a frame`, `tests/gateway.rs:30`)
  - delivery_resumes_after_a_warning: falhou em 5.2 s (`timed out sending a frame`, `tests/gateway.rs:30`)
  - slow_receiver_does_not_hold_back_others: falhou em 5.1 s (`timed out waiting for a message`, `tests/gateway.rs:22`)
  - invalid_json_is_rejected_without_broadcasting: passou
- contraste em main (mesma mutacao):
  - slow_consumer_receives_a_warning_frame: TRAVOU (morto pelo alarme em 60 s)
  - delivery_resumes_after_a_warning: TRAVOU (morto pelo alarme em 60 s)
  - slow_receiver_does_not_hold_back_others: falhou em 5.1 s (`timed out waiting for a message`)
  - os outros 9: passaram
- isolamento: `git status --porcelain` da worktree de verificacao estava vazio antes e depois. Worktrees e target dirs temporarios foram removidos.

## Quem rodou, e quando
- comando: cargo fmt --check && cargo clippy --all-targets -- -D warnings && cargo test
- resultado: verde em HEAD (057f3c6) em 2026-10-05. Observacao: o commit 80d876d sozinho falha no clippy com `function send is never used` (`tests/gateway.rs:27`), porque o helper entra antes de ser usado; o commit seguinte, 34de502, resolve isso. O gate vale para HEAD; quem quiser cada commit verde deve juntar os dois.
- testes antes/depois: main 19 (12 gateway + 7 unitarios do loadgen); HEAD 19 (12 + 7). Mesmos nomes.
- estabilidade: `cargo test --test gateway` 10 vezes seguidas em HEAD, 10/10 verdes (12 passaram em cada uma), entre 1.68 s e 2.23 s por rodada. Em main a mesma suite levou 2.93 s numa rodada unica; nenhuma lentidao mensuravel com o timer extra por envio.
- CI: .github/workflows/ci.yml roda os mesmos tres comandos no PR; main nao tem protecao de branch, entao quem revisa precisa confirmar o verde antes do merge
- sessao independente: subagente verificador em sessao nova, sem o historico da implementacao

## Veredito
aprovado
