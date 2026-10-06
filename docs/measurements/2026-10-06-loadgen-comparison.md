# Loadgen comparison, 2026-10-06

Raw record behind the old-versus-new `loadgen` claims in `docs/architecture.md`.
The same gateway binary was driven by two `loadgen` builds: old (`2612f71`) and
new (`0bc858b`). Apple M4 Pro, 14 cores, `--release`, same machine, not idle
(1-minute load average 3.4-11 for `goal-1m-mesh`, 11-20 for the `cliff-300`
repeats). Each pair ran back to back. CPU per frame is loadgen user plus sys
CPU time over frames received.

Only the difference between the two builds was measured. The cause of the old
`cliff-300` results was not isolated.

## goal-1m-mesh

Invocation: `--connections 201 --senders 50 --rate 100`

| Build | cpu_s | user | sys | received | us per frame | service p99 ms | warnings | delivery % |
|---|---|---|---|---|---|---|---|---|
| old | 69.77 | 11.96 | 57.81 | 13,000,000 | 5.367 | 6.256 | 0 | 100.0 |
| new | 22.37 | 12.04 | 10.33 | 13,000,000 | 1.721 | 6.192 | 0 | 100.0 |
| old | 68.84 | 12.27 | 56.57 | 13,000,000 | 5.295 | 6.48 | 0 | 100.0 |
| new | 22.46 | 12.35 | 10.11 | 13,000,000 | 1.728 | 7.024 | 0 | 100.0 |
| old | 69.14 | 12.0 | 57.14 | 13,000,000 | 5.318 | 6.672 | 0 | 100.0 |
| new | 21.97 | 11.68 | 10.29 | 13,000,000 | 1.690 | 6.16 | 0 | 100.0 |

## cliff-300

Invocation: `--connections 300 --senders 30 --rate 400`

| Build | cpu_s | user | sys | received | us per frame | p99 ms | warnings | delivery % |
|---|---|---|---|---|---|---|---|---|
| old | 158.08 | 23.33 | 134.75 | 33,622,357 | 4.702 | 575.488 | 206 | 72.0829 |
| new | 43.87 | 22.54 | 21.33 | 46,413,887 | 0.945 | 21.184 | 5081 | 99.5067 |
| old | 144.07 | 23.27 | 120.8 | 32,885,468 | 4.381 | 686.08 | 481 | 70.5031 |
| new | 53.53 | 24.41 | 29.12 | 46,644,000 | 1.148 | 3.848 | 0 | 100.0 |
| old | 142.43 | 23.12 | 119.31 | 33,939,038 | 4.197 | 661.504 | 868 | 72.7619 |
| new | 52.96 | 24.25 | 28.71 | 46,636,287 | 1.136 | 3.896 | 80 | 99.9835 |

## Commands

Each run is the `loadgen` example of the named commit against a running
gateway, with the invocation above:

```bash
cargo run --release --example loadgen -- <invocation>
```

The old build is `2612f71` and the new build is `0bc858b`. The gateway binary
was the same for both. The runs were made by a local comparison script whose
output is the table above, one line per run in the order shown. The script
itself was not kept, and `--seconds` and `--warmup` were not recorded in its
output (13,000,000 frames received at 201 connections fits 13 measured seconds).
