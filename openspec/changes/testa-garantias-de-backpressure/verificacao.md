# Verificacao de testa-garantias-de-backpressure

Requisito sem citacao conta como NAO coberto -- nao como provavelmente coberto.

### Lagging connections are warned of dropped frames

- prova (Slow consumer receives a warning): `tests/gateway.rs:129` -- `assert!(warning["dropped"].as_u64().unwrap() > 0)`, depois do laço em `tests/gateway.rs:123-128` que le B ate achar `msg["type"] == "warning"`; a carga e `BROADCAST_CAPACITY * 500` = 128000 frames (`tests/gateway.rs:116`), igual ao WHEN.
- prova (Delivery resumes after a warning), clausula "B receives at least one `warning`": `tests/gateway.rs:167` -- `assert!(warnings > 0, "receiver never lagged, so resumption was not exercised")`, com `tests/gateway.rs:153` -- `assert!(msg["dropped"].as_u64().unwrap() > 0, "{msg}")` em cada warning.
- prova (Delivery resumes after a warning), clausula "every `seq` B receives is strictly greater than the previous one": `tests/gateway.rs:160` -- `assert!(seq > prev, "seq {seq} arrived after {prev}")`; qualquer frame que nao seja warning, seq ou marcador cai no `panic!("unexpected frame: {msg}")` de `tests/gateway.rs:164`.
- prova (Delivery resumes after a warning), clausula "B receives the `{"marker":"end"}` frame": `tests/gateway.rs:156` -- `Some("message") if msg["data"] == json!({ "marker": "end" }) => break`; e a unica saida do laço, e cada leitura usa o timeout de 5 s de `next_msg` (`tests/gateway.rs:20-22`). Sem o marcador, o teste falha por timeout ou por erro do stream, nunca passa.

### Publishing never waits for a slow receiver

- prova (Other connections keep receiving), clausula "C receives every `seq` from 0 to 127999 in order": `tests/gateway.rs:189` -- `assert_eq!(msg["data"], json!({ "seq": i }), "{msg}")`, para cada `i` de 0 a 127999 em chunks de 100 (`tests/gateway.rs:180-191`), em lockstep com o envio de A.
- prova (Other connections keep receiving), clausula "followed by `{"marker":"end"}`": `tests/gateway.rs:195` -- `assert_eq!(next_json(&mut c).await["data"], json!({ "marker": "end" }))`.
- prova (Other connections keep receiving), clausula "B, when it reads again, receives a `warning`": `tests/gateway.rs:197-206` -- o laço so termina em `msg["type"] == "warning"` (`tests/gateway.rs:199`) e `assert_ne!(msg["data"], json!({ "marker": "end" }), ...)` (`tests/gateway.rs:202`) falha se B chegar ao marcador sem ter recebido warning.

Observacoes (nao bloqueiam):

- Lacuna de precisao da spec: o texto do primeiro requisito diz que `n` e "equal to the number of frames discarded", mas nenhum cenario descreve essa igualdade e nenhum teste a confere. O design.md declara isso como fora do objetivo, porque a contagem exata depende de agendamento. Hoje so `n > 0` tem cobertura. Corrigir no texto do requisito (ou criar um cenario mensuravel) numa change futura.
- O mutante m2 fez `delivery_resumes_after_a_warning` e `slow_consumer_receives_a_warning_frame` travarem sem fim: `a.send(...)` nao tem timeout, e A bloqueia quando o leitor do servidor para. Numa regressao desse tipo, `cargo test` trava em vez de falhar, ate o timeout do job de CI. O teste que mira essa garantia (`slow_receiver_does_not_hold_back_others`) falha corretamente em 5 s.

## Sensor de discriminacao

Cada mutacao rodou numa worktree isolada (`git worktree add /tmp/bp-mN HEAD`), cada uma com seu proprio `CARGO_TARGET_DIR`, e cada execucao recompilou `realtime-gateway` a partir da copia mutada. Um primeiro lote que compartilhava o mesmo target dir foi descartado: o cargo reaproveitou o binario do primeiro mutante para os outros. `git status --porcelain` da worktree real: vazio antes, vazio depois.

- mutacao: `src/ws.rs:82-85`, braco `Lagged` -- envia o warning e da `break` no laço da ponte, em vez de seguir entregando (task 3.3)
- resultado: o teste morreu -- `delivery_resumes_after_a_warning` falhou em `tests/gateway.rs:24` com `Protocol(ResetWithoutClosingHandshake)`: depois do warning, a entrega para B parou e a conexao caiu antes do marcador.
- mutacao: `src/ws.rs:114`, antes do `tx_global.send` -- `while tx_global.len() >= BROADCAST_CAPACITY - 1 { sleep(1ms) }`, ou seja, publicar passa a esperar o assinante mais lento (task 3.3)
- resultado: o teste morreu -- `slow_receiver_does_not_hold_back_others` falhou em 5,1 s com `timed out waiting for a message`, a partir de `next_json(&mut c)` em `tests/gateway.rs:188` (confirmado por backtrace): A travou atras de B e C parou de receber. Os outros dois testes de lag travaram, como descrito nas observacoes, e o processo foi encerrado pelo limite de 400 s.
- mutacao: `src/ws.rs:82-85`, braco `Lagged` -- `continue` sem emitir warning (mutacao escolhida pelo verificador; testa as autoverificacoes de precondicao)
- resultado: o teste morreu -- `delivery_resumes_after_a_warning` falhou em `tests/gateway.rs:167` ("receiver never lagged, so resumption was not exercised"); `slow_receiver_does_not_hold_back_others` falhou em `tests/gateway.rs:202` ("the idle receiver never fell behind, so the test exercised nothing"); `slow_consumer_receives_a_warning_frame` falhou por timeout.
- mutacao: `src/ws.rs:84` -- `ServerMessage::Warning { dropped: 0 }` (mutacao escolhida pelo verificador)
- resultado: o teste morreu -- `delivery_resumes_after_a_warning` falhou em `tests/gateway.rs:153`; `slow_consumer_receives_a_warning_frame` falhou em `tests/gateway.rs:129`. `slow_receiver_does_not_hold_back_others` sobreviveu, o que e esperado: ele so confere o tipo do frame, e o cenario dele nao fixa `dropped`.

## Quem rodou, e quando

- comando: cargo fmt --check && cargo clippy --all-targets -- -D warnings && cargo test
- resultado: pass (fmt limpo, clippy sem avisos, todas as suites ok), em 2026-10-05 sobre 7e8c9ff
- testes antes/depois: 14 na base 0edcbf3 (gateway 7 + loadgen 7) / 16 em HEAD (gateway 9 + loadgen 7)
- estabilidade: 16 execucoes de `cargo test --test gateway` (15 seguidas + a do gate), 0 falhas, de 1,7 s a 3,9 s cada
- validacao: `openspec validate testa-garantias-de-backpressure --strict` ok; a validacao de regras das changes passou
- escopo: o diff `main...HEAD` toca apenas `tests/gateway.rs` (+78, dois testes novos), o delta da spec (linhas `Teste:`) e `tasks.md`; nenhuma mudanca em `src/`, como declara proposal.md
- CI: .github/workflows/ci.yml roda os mesmos tres comandos no PR; main nao tem protecao de branch, entao o revisor precisa confirmar o verde antes do merge
- sessao independente: subagente verificador em sessao nova, sem o historico da implementacao

## Veredito

aprovado -- todo cenario tem prova `arquivo:linha` com asserção exata, os dois mutantes exigidos pela task 3.3 mataram o teste-alvo, os dois mutantes extras tambem foram detectados, e o gate passou de forma estavel. Ficam registradas, sem bloquear, a lacuna de precisao sobre `dropped` igual ao numero descartado e o travamento sem timeout dos envios de A nos testes de lag.
