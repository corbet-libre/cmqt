# Relay datagram format, versions 1 and 2

`cmqt flush` (on the relay) sends, `cmqt unseal` (on the box) receives. The
path is one-way UDP; the box never answers. Everything below is big endian.

```text
offset  size  field
0       1     version, 1 (plaintext line protocol) or 2 (plaintext is one zstd frame of line protocol)
1       4     sender prefix: random per process start
5       8     sequence number: 0, 1, 2 ... per prefix, one per datagram
13      n+16  ChaCha20-Poly1305 ciphertext followed by the 16-byte tag
```

| Item | Rule |
|---|---|
| Key | One static 32-byte key. File holds 32 raw bytes or 64 hex characters. The file must have no group/other permission bits (0600); both tools refuse otherwise, except a systemd credential (0440 under `/run/credentials/`). |
| Nonce | 12 bytes: prefix (4) followed by sequence (8), taken from the header. A new prefix on every start keeps nonces unique across restarts (collision odds 2^-32 per restart pair); the counter keeps them unique within a run. |
| AAD | The 13 header bytes, so version, prefix and sequence are authenticated. |
| Plaintext | InfluxDB line protocol, newline-terminated lines, at most 1100 bytes. A line never splits across datagrams. A datagram is at most 1129 bytes, under the IPv6 minimum MTU of 1280. |
| Version 2 | The sealed plaintext is ONE complete zstd frame (level 3) of newline-terminated line protocol, filled with as many lines as keep the COMPRESSED size at or below 1100 bytes. Every datagram is compressed on its own: none depends on another, so a lost datagram loses only its own lines. The receiver decompresses with a limit of 256 KiB; a bad or larger frame counts as `malformed`. The sender (HTTP mode of `cmqt flush`) seals version 2; the legacy RabbitMQ consumer mode still seals version 1. A receiver accepts both. |
| Version | A receiver rejects any version it does not know (`malformed`). A format change bumps the byte. |

## Receiver accounting

| Counter | Meaning |
|---|---|
| `received` | Authenticated and accepted datagrams. |
| `gap` | Sum of sequence jumps per prefix (datagrams presumed lost). The first datagram of a prefix creates no gap. |
| `late` | Accepted out of order after its gap was counted. Net loss = `gap - late`. |
| `replays` | Authenticated but already seen, or more than 128 behind the newest. Dropped. |
| `auth_failures`, `malformed` | Bad tag or key; too short or unknown version. Dropped. |

The replay window is 128 sequence numbers per sender prefix; the 8 newest
prefixes are tracked. Loss while the box is down is the outage itself: UDP
has no acknowledgement and the sender cannot see it.

## Line protocol shape

VictoriaMetrics must run with `-influxSkipSingleField` so the series name is
the measurement. Metric names and attribute keys use the Prometheus
charset (other characters become `_`). Resource attributes are tags.

| OTLP | Lines |
|---|---|
| gauge, sum | `name value=X ts` |
| histogram | `name_bucket,le=B` (cumulative, `+Inf` last), `name_sum`, `name_count` |
| summary | `name,quantile=Q`, `name_sum`, `name_count` |
| exponential histogram | `name_sum`, `name_count`; buckets dropped, counted in `cmqt_flush_exp_histogram_buckets_dropped_total` |
