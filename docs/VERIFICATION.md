# Verification

Date: 17 October 2026. Windows x86_64 MSVC, Rust 1.94.0 (4a4ef493e, 2026-03-02), Cargo 1.94.0. CPU: Intel Core i7-8565U @ 1.80 GHz. Commands ran from the repository root using the committed lockfile.

| Command | Result / scope |
|---|---|
| `cargo test --locked` | Passed: 14 integration tests, 0 failures. Queue priority/overflow/degraded behavior, deadline calculations, parsing, leaderboard and state transitions; no network. Library/binary/doc test targets contain 0 additional tests. |
| `cargo run --locked -- --heap-proof` | Passed offline. Captured actual allocation counts from 10,000 parses of the built-in fictional unescaped event per representation; see transcript below. |
| `git diff --check` | Passed before commit. |

The parser allocation proof is an unoptimized debug run, with the application's tracking global allocator. It is an allocation count demonstration, not a throughput or live deadline benchmark. The fixture has unescaped strings; it does not establish support for every public event payload.

Capture command (PowerShell):

```powershell
cargo run --locked -- --heap-proof | Set-Content -Encoding utf8 docs/assets/heap-proof.txt
```

`docs/assets/heap-proof.png` is a Pillow/Consolas rendering of the captured stdout with the command and environment added as labels. It is not a screenshot of the ratatui dashboard. The image and text contain only the built-in fictional example.

No live network run, `--capture`, dashboard screenshot or full Criterion suite was performed. Existing `logs/run_20260510_*.log` and saved public event files are unchanged historical material. Live SSE reconnection, chunk boundaries, terminal rendering and OS-scheduler deadlines remain outside this check; known limitations are retained in `PROJECT_GUIDE.md`.
