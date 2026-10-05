# Verificacao de testa-relay-byte-a-byte

Requisito sem citacao conta como NAO coberto -- nao como provavelmente coberto.

### Text frames are relayed in an envelope

- Scenario "Object payload reaches another connection" -- prova: `tests/gateway.rs:60` -- `assert_eq!(next_json(&mut b).await, json!({"type": "message", "from": a_id, "data": payload}))`, com o payload `{"hp":42,"pos":[1,2],"nested":{"any":null}}` enviado em `tests/gateway.rs:57-58`. A comparacao e semantica (`Value`), o que basta para este cenario: ele nao exige bytes exatos. Os bytes exatos ficam com os dois cenarios abaixo.
- Scenario "Payload bytes are kept exactly" -- prova parcial: `tests/gateway.rs:75` -- `assert_eq!(next_msg(&mut b).await.into_text().unwrap().as_str(), format!(r#"{{"type":"message","from":"{a_id}","data":{payload}}}"#))`, comparando o texto cru do frame. LACUNA: o payload do teste (`tests/gateway.rs:73`) e `{"b":1,  "a":[ 1,2 ],"u":"\u00e9","n":1.50}`, com o escape `\u00e9` (6 bytes ASCII), enquanto o cenario da spec (e a tarefa 1.1) trazem `"u":"é"` literal em UTF-8 (bytes `C3 A9`). O texto exato do cenario nao e exercitado. O teste e mais forte que o cenario nesse ponto (um round trip por `Value` reescreve `\u00e9` para `é`, mas mantem `é` literal), e o design.md pede "a `\u` escape". Ou seja, a divergencia esta na spec e na tarefa 1.1, nao no teste.
- Scenario "Surrounding whitespace is dropped" -- prova: `tests/gateway.rs:80` envia `"  42  "` e `tests/gateway.rs:81` -- `assert_eq!(next_msg(&mut b).await.into_text().unwrap().as_str(), format!(r#"{{"type":"message","from":"{a_id}","data":42}}"#))`, que compara o texto exato.

Nenhum dos tres cenarios usa asserção rasa: nenhum para em "nao deu erro", nenhum depende de mock, e o valor esperado e montado no proprio teste, sem constante importada do codigo sob teste.

## Sensor de discriminacao

Cada mutacao rodou numa worktree isolada (`git worktree add --detach /tmp/vrb-mN HEAD`, com `CARGO_TARGET_DIR` proprio), com `cargo test --test gateway`. O `git status --porcelain` da worktree de verificacao estava vazio antes e continuou vazio depois. As worktrees temporarias foram removidas.

- mutacao: `src/ws.rs:99`, round trip de `data` por `serde_json::Value` (`RawValue::from_string(serde_json::from_str::<Value>(data.get())?.to_string())`), que e a mutacao pedida na tarefa 3.3
- resultado: o teste morreu. `text_payload_bytes_are_relayed_verbatim` falhou em `tests/gateway.rs:75`: chegou `"data":{"a":[1,2],"b":1,"n":1.5,"u":"é"}` no lugar dos bytes originais. `payload_is_relayed_verbatim_to_others` continuou passando, como a tarefa preve (7 passed, 1 failed).
- mutacao: `src/ws.rs:99`, envelope montado com o texto cru do frame (`format!(... "data":{}}}, text.as_str())`), sem descartar o espaco em volta do valor
- resultado: o teste morreu. `text_payload_bytes_are_relayed_verbatim` falhou em `tests/gateway.rs:81`: chegou `"data":  42  ` no lugar de `"data":42`. Os outros 7 testes passaram.
- mutacao: `src/protocol.rs:9`, ordem dos campos de `ServerMessage::Message` trocada para `{ data, from }`, o que muda a ordem das chaves do envelope
- resultado: o teste morreu. `text_payload_bytes_are_relayed_verbatim` falhou em `tests/gateway.rs:75`: chegou `{"type":"message","data":...,"from":...}`. `payload_is_relayed_verbatim_to_others` passou.

## Quem rodou, e quando

- comando: cargo fmt --check && cargo clippy --all-targets -- -D warnings && cargo test
- resultado: pass (fmt limpo, clippy sem avisos, todos os testes verdes), rodado em 2026-10-05 sobre o HEAD `6362841`
- testes antes/depois: 14 no main (7 em `tests/gateway.rs` + 7 unitarios) / 15 no HEAD (8 + 7)
- CI: .github/workflows/ci.yml roda os mesmos tres comandos no PR. O main nao tem branch protection, entao quem revisa precisa confirmar o CI verde antes do merge.
- sessao independente: subagente verificador em sessao nova, sem o historico da implementacao
- validacao: `openspec validate testa-relay-byte-a-byte --strict` passou, e a validacao de regras das changes tambem

Escopo: o diff `main...HEAD` toca apenas `tests/gateway.rs` (teste novo), o delta da spec (linha `Teste:`) e `tasks.md`. Nao ha mudanca em `src/`, o que bate com a proposal.md.

## Veredito

lacunas:

1. O cenario "Payload bytes are kept exactly" (e a tarefa 1.1) especifica `"u":"é"` literal, mas o teste envia `"u":"\u00e9"`. Isso e uma lacuna de precisao da spec. A correcao e alinhar o cenario e a tarefa 1.1 ao escape `\u00e9`, como o design.md ja pede, e nao mexer no teste. Feito isso, o cenario fica coberto por `tests/gateway.rs:75`.

Fora isso, o gate passa, os tres mutantes foram mortos e o escopo esta correto. A tarefa 3.3 continua aberta ate a spec ser corrigida.
