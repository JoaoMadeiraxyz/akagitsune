# Verificacao de fixa-carga-do-teste-de-warning

Change sem requisitos (skip_specs). Cada objetivo da proposta precisa de prova arquivo:linha.

`openspec validate fixa-carga-do-teste-de-warning --strict`: valido (skip_specs, zero deltas aceitos).

### A carga do teste e a do cenario: 128000 frames
- prova: `tests/gateway.rs:180` -- `for i in 0..128_000 {` envia exatamente 128000 frames `{"seq":i}` de A (`tests/gateway.rs:181`), como diz o cenario "Slow consumer receives a warning" (`openspec/specs/delivery-backpressure/spec.md:15`).

### O teste nao le mais `BROADCAST_CAPACITY`
- prova: `rg BROADCAST_CAPACITY tests` nao retorna nada (exit 1). A constante so aparece em `src/state.rs:7` e `src/state.rs:22`.

### Asserções e nome inalterados
- prova: `tests/gateway.rs:175` -- nome `slow_consumer_receives_a_warning_frame` igual ao de `main`.
- prova: `tests/gateway.rs:184-189` -- laco ate `msg["type"] == "warning"` e `assert!(warning["dropped"].as_u64().unwrap() > 0)`, identicos a `main`. O cenario define `n > 0`, e a asserção mira exatamente isso.
- prova: `git diff main...HEAD -- tests/gateway.rs` troca so as duas linhas do `overflow` pela linha do laco literal (1 insercao, 2 remocoes).

### Escopo
- prova: `git diff --stat main...HEAD` toca apenas `tests/gateway.rs` e `openspec/changes/fixa-carga-do-teste-de-warning/tasks.md`. Nada em `src/`.

## Sensor de discriminacao
Cada mutacao numa worktree isolada (`git worktree add --detach`) com `CARGO_TARGET_DIR` proprio; as tres recompilaram `realtime-gateway` (`Compiling realtime-gateway` no log). Binario rodado direto sob `perl -e 'alarm N; exec @ARGV' <binario> slow_consumer_receives_a_warning_frame --exact`.
- mutacao a: HEAD com `BROADCAST_CAPACITY = 1 << 18` (`src/state.rs:7`).
- resultado a: FALHOU em 7 s de parede (exit 101), panic em `tests/gateway.rs:22` "timed out waiting for a message": 128000 frames cabem no bus, nenhum warning chega e o timeout de 5 s do recebimento dispara. Mutante morto dentro dos 60 s.
- mutacao b (contraste): `main` com `BROADCAST_CAPACITY = 1 << 18`.
- resultado b: nao terminou; morto pelo alarme aos 90 s (exit 142), "has been running for over 60 seconds". Confirma o problema descrito na proposta.
- mutacao c: HEAD com `BROADCAST_CAPACITY = 512`.
- resultado c: passou em 1 s (exit 0); 128000 frames ainda estouram o bus.
- arvore real: `git status --porcelain` vazio antes e depois; worktrees e target dirs temporarios removidos.

## Quem rodou, e quando
- comando: cargo fmt --check && cargo clippy --all-targets -- -D warnings && cargo test
- resultado: verde em HEAD (cce2e74), 2026-10-05: fmt limpo, clippy sem avisos, todos os testes passam.
- testes antes/depois: 20 em `main` (13 em `tests/gateway.rs` + 7 em `examples/loadgen.rs`), 20 em HEAD (13 + 7).
- estabilidade: `cargo test --test gateway` 10 vezes seguidas em HEAD, 10/10 com 13 passed.
- CI: .github/workflows/ci.yml runs the same three commands on the PR; main has no branch protection, so the reviewer must confirm green before merging
- sessao independente: subagente verificador em sessao nova, sem o historico da implementacao

## Veredito
aprovado
