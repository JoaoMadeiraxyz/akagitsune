# Verificacao de testa-relay-byte-a-byte

Requisito sem citacao conta como NAO coberto -- nao como provavelmente coberto.

Rodada 2. A rodada 1 (commit `216b4d0`) apontou que o cenario "Payload bytes are kept exactly" e a tarefa 1.1 traziam um `é` literal em UTF-8, enquanto o teste envia o escape `\u00e9`. A causa foi um erro de encoding ao escrever os artefatos. O commit `df3c22a` trocou o `é` pelo escape `\u00e9` em proposal.md, design.md, tasks.md e no delta spec, sem tocar `tests/` nem `src/` (`git diff df3c22a~1 df3c22a -- src tests` vazio). Esta rodada reconfere o cenario e refaz o gate. Como o codigo e os testes sao os mesmos da rodada 1, os resultados do sensor continuam valendo.

### Text frames are relayed in an envelope

- Scenario "Object payload reaches another connection" -- prova: `tests/gateway.rs:60` -- `assert_eq!(next_json(&mut b).await, json!({"type": "message", "from": a_id, "data": payload}))`, com o payload `{"hp":42,"pos":[1,2],"nested":{"any":null}}` enviado em `tests/gateway.rs:57-58`. A comparacao e semantica (`Value`), o que basta para este cenario: ele nao exige bytes exatos. Os bytes exatos ficam com os dois cenarios abaixo.
- Scenario "Payload bytes are kept exactly" -- prova: `tests/gateway.rs:75` -- `assert_eq!(next_msg(&mut b).await.into_text().unwrap().as_str(), format!(r#"{{"type":"message","from":"{a_id}","data":{payload}}}"#))`, comparando o texto cru do frame, com o payload de `tests/gateway.rs:73`. Na rodada 2, o payload do WHEN do cenario (delta spec, linha 16) e o literal do teste foram comparados com `xxd` e sao identicos byte a byte, inclusive o escape `"u":"\u00e9"` (bytes `5c 75 30 30 65 39`). O THEN (linha 17) e igual ao `format!` esperado com esse payload. O delta spec ja nao contem nenhum `é` literal.
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
- resultado: pass nas duas rodadas. Rodada 1 sobre `6362841`, rodada 2 sobre `df3c22a`, ambas em 2026-10-05. A validacao `--strict` e a validacao de regras das changes tambem passaram de novo.
- testes antes/depois: 14 no main (7 em `tests/gateway.rs` + 7 unitarios) / 15 no HEAD (8 + 7)
- CI: .github/workflows/ci.yml roda os mesmos tres comandos no PR. O main nao tem branch protection, entao quem revisa precisa confirmar o CI verde antes do merge.
- sessao independente: subagente verificador em sessao nova, sem o historico da implementacao
- validacao: `openspec validate testa-relay-byte-a-byte --strict` passou, e a validacao de regras das changes tambem

Escopo: o diff `main...HEAD` toca apenas `tests/gateway.rs` (teste novo) e os artefatos da change: o delta da spec (linha `Teste:` e o escape `\u00e9`), `tasks.md`, `proposal.md`, `design.md` e este relatorio. Nao ha mudanca em `src/`, o que bate com a proposal.md.

## Veredito

aprovado. A lacuna da rodada 1 foi fechada com o alinhamento da spec ao teste (`df3c22a`). Os tres cenarios tem prova `arquivo:linha` com asserção exata, o gate passa, os tres mutantes foram mortos e o escopo bate com a proposal.md.
