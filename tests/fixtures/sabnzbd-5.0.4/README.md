# SABnzbd 5.0.4 API goldens

These normalized JSON responses define RustNZB's SAB-compatible response
contract. They were captured from the supported SABnzbd 5.0.4 release
(`128e0d03d7cc61af7e73b18376b880219fbc3596`) using the LinuxServer image:

```text
lscr.io/linuxserver/sabnzbd:5.0.4
sha256:302be8972d4627222a0701634f2f9025826d760d856414aa6e67c0a66833e5be
```

The key sets and types were cross-checked against SABnzbd's own strict Tavern
fixtures at tag `5.0.4`:

- `tests/data/tavern/api_queue_empty.yaml`
- `tests/data/tavern/api_queue_format.yaml`
- `tests/data/tavern/api_history_empty.yaml`
- `tests/data/tavern/api_history_format.yaml`
- `tests/data/tavern/api_version.yaml`
- `sabnzbd/api.py::build_status` (there is no upstream fullstatus Tavern case)

`$type:*` markers replace only environment- or time-dependent values. The
conformance helper verifies each marker's original JSON type before
normalization, then compares the full document so extra and missing keys fail.
