# Verificacao de testa-contagem-exata-de-dropped

Requisito sem citacao conta como NAO coberto -- nao como provavelmente coberto.

Escopo julgado: apenas os commits da change sobre a base empilhada (`spec/implementa-limita-tempo-de-envio-nos-testes..HEAD`: 36c7363, aaa34ff, a681998). O diff toca so `tests/gateway.rs` (+50), a linha `Teste:` do delta e `tasks.md`. Nenhum arquivo em `src/`. `openspec validate testa-contagem-exata-de-dropped --strict`: valido.

### Lagging connections are warned of dropped frames
- prova (Slow consumer receives a warning): `tests/gateway.rs:191` -- `assert!(warning["dropped"].as_u64().unwrap() > 0)`, alcancado so depois do laco em `tests/gateway.rs:185-189` encontrar um frame `type == "warning"`; carga de `BROADCAST_CAPACITY * 500` = 128000 frames enviados por A sem B ler.
- prova (Delivery resumes after a warning): `tests/gateway.rs:230` -- `assert!(warnings > 0, ...)` (ao menos um warning); `tests/gateway.rs:222` -- `assert!(seq > prev, ...)` (cada `seq` estritamente maior que o anterior); `tests/gateway.rs:218` -- o laco so termina no braco `Some("message") if msg["data"] == json!({ "marker": "end" }) => break`, e `next_json` falha por timeout de 5 s se o marcador nunca chegar.
- prova (Dropped count is exact): `tests/gateway.rs:279` -- `assert!(warnings > 0, "receiver never lagged, so the count was not exercised")`; `tests/gateway.rs:270` -- `assert_eq!(seq, expected_next + dropped_since_last, ...)`, com `expected_next` iniciando em 0 e virando `seq + 1` a cada frame, e `dropped_since_last` acumulando todos os warnings desde o `seq` anterior e zerando depois (forma do gap, inclusive warnings consecutivos); `tests/gateway.rs:282` -- `assert_eq!(received + dropped_total, total + 1)` com `total = 128_000` e `received` contando o marcador (forma do balanco, 128001).

Nao vacuidade: com `BROADCAST_CAPACITY` elevado para `1 << 18` (sem lag possivel), `dropped_count_matches_the_frames_skipped` falha em `tests/gateway.rs:278` com "receiver never lagged, so the count was not exercised". O teste nao passa quando nao ha lag.

Asserções rasas: nenhuma nos cenarios novos; os valores esperados (`total + 1`, `expected_next`) sao derivados da carga do proprio teste, nao de constantes do codigo sob teste. O cenario 1 usa `BROADCAST_CAPACITY` so para dimensionar a carga, e o resultado que ele exige (`n > 0`) e o que o cenario define.

Lacunas de precisao da spec: nenhuma. Observacao fora do escopo desta change: o laco de `slow_consumer_receives_a_warning_frame` (`tests/gateway.rs:185`) nao tem prazo total; no mutante sem lag o binario de teste ficou girando a 100% de CPU por mais de 10 minutos e teve de ser morto. Ele so quebra o laco num warning, entao, se o lag nunca acontece, ele depende de `next_json` dar timeout, o que nesse mutante nao aconteceu na pratica. Vale avaliar numa change separada.

## Sensor de discriminacao

Cada mutante rodou numa worktree isolada (`git worktree add --detach /tmp/... HEAD`), com `CARGO_TARGET_DIR` proprio; em cada execucao apareceu `Compiling realtime-gateway` do caminho do mutante (recompilou). Cada execucao foi limitada por alarme.

- mutacao: `src/ws.rs:84`, warning envia `dropped: dropped - 1`
- resultado: o teste morreu. `dropped_count_matches_the_frames_skipped` falhou em `tests/gateway.rs:270` (left 127745, right 127744). `slow_consumer_receives_a_warning_frame` passou, como esperado; `delivery_resumes_after_a_warning` tambem passou.

- mutacao: `src/ws.rs:84`, warning envia `dropped: dropped + 1`
- resultado: o teste morreu. `dropped_count_matches_the_frames_skipped` falhou em `tests/gateway.rs:270` (left 127745, right 127746). `slow_consumer_receives_a_warning_frame` passou, como esperado; `delivery_resumes_after_a_warning` tambem passou.

- mutacao (propria, frame perdido sem contagem logo apos o lag): `src/ws.rs:83`, inserido `let _ = rx_global.recv().await;` no braco `Lagged`, descartando em silencio o proximo frame do bus; o `dropped` continua correto
- resultado: o teste morreu. `dropped_count_matches_the_frames_skipped` falhou em `tests/gateway.rs:270` (left 127746, right 127745). Os outros 12 testes passaram, inclusive `delivery_resumes_after_a_warning` (a monotonia do `seq` nao percebe a perda).

- mutacao (propria, perda no fim do fluxo, que a forma do gap nao ve): `src/ws.rs:79-85`, a ponte guarda `lagged = true` no braco `Lagged` e depois descarta o frame recebido quando `lagged && rx_global.len() == 1`, ou seja, o ultimo `seq` antes do marcador
- resultado: o teste morreu, 3 de 3 execucoes. `dropped_count_matches_the_frames_skipped` falhou na forma do balanco em `tests/gateway.rs:282` (left 128000, right 128001); a forma do gap sobreviveu, o que confirma a justificativa do design.md para ter as duas formas. Os outros 12 testes passaram.

- mutacao (nao vacuidade): `src/state.rs:7`, `BROADCAST_CAPACITY` de 256 para `1 << 18`
- resultado: o teste morreu em `tests/gateway.rs:278` ("receiver never lagged").

`git status --porcelain` da worktree de verificacao: vazio antes e depois. Worktrees temporarias e diretorios de build removidos.

## Quem rodou, e quando
- comando: cargo fmt --check && cargo clippy --all-targets -- -D warnings && cargo test
- resultado: fmt limpo, clippy limpo, cargo test verde (2026-10-05, HEAD a681998)
- testes antes/depois: base `spec/implementa-limita-tempo-de-envio-nos-testes` (057f3c6) 19 testes (12 em `tests/gateway.rs` + 7 em `examples/loadgen.rs`); HEAD 20 testes (13 + 7). +1, o teste novo.
- estabilidade: `cargo test --test gateway` 10 vezes seguidas em HEAD, 10/10 com 13 passed (1.66 s a 2.23 s)
- CI: .github/workflows/ci.yml runs the same three commands on the PR; main has no branch protection, so the reviewer must confirm green before merging
- sessao independente: subagente verificador em sessao nova, sem o historico da implementacao

## Veredito
aprovado
