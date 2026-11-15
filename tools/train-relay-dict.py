#!/usr/bin/env python3
"""Train the zstd dictionary of relay datagram format v3 from recorded line protocol.

usage: train-relay-dict.py lines.rec crates/cmqt-relay/dict/relay-v3.zdict
Record the input with `record_lines = "<file>"` in the flush config (real device traffic, 8 MiB cap),
then retrain and bump the format version if the dictionary changes (docs/relay-datagram.md).
Needs python-zstandard (run with `python3 -I`).
"""
import sys
import zstandard as z

CHUNK = 60  # lines per training sample, about one datagram's worth
DICT_ID = 3
SIZE = 65536
lines = open(sys.argv[1], "rb").read().splitlines(keepends=True)
samples = [b"".join(lines[i:i + CHUNK]) for i in range(0, len(lines), CHUNK)]
d = z.train_dictionary(SIZE, samples, dict_id=DICT_ID)
open(sys.argv[2], "wb").write(d.as_bytes())
print(f"{len(samples)} samples, dictionary {len(d.as_bytes())} bytes, id {DICT_ID}")
