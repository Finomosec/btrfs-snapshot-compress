# btrfs-snapshot-compress

Compress btrfs file extents with **reflink-preservation**. The tool defrag-compresses each live extent with zstd and immediately re-points all reflink siblings (snapshots, `cp --reflink=always` copies, prior duperemove output) at the new compressed extent — so compression gains are kept **and** sharing stays intact.

Sister project to [btrfs-snapshot-dedup](https://github.com/Finomosec/btrfs-snapshot-dedup). Both share the same parameter style, find-filter logic, versioning conventions, and 10s status interval.

## Why a separate tool?

Naive `btrfs filesystem defragment -czstd` rewrites extents → snapshot/reflink siblings keep pointing at the old uncompressed data → on-disk size **grows** until the old extents are fully orphaned. On a near-full volume this can fill the disk before any savings show up.

This tool does the work per-extent and atomically:

1. Probe-compress the first 128 KB of the extent → bail out cheaply if not compressible
2. `LOGICAL_INO_V2` reverse-lookup → find every file that references this physical extent
3. `BTRFS_IOC_DEFRAG_RANGE` with `COMPRESS|ZSTD` → rewrite the extent compressed
4. `FIDEDUPERANGE` for every reflink sibling → all references pulled to the new compressed extent
5. Old uncompressed extent becomes orphan → kernel frees it

The "danger window" is bounded by a single extent (typ. ≤ 128 MB), not a whole file.

## How it differs from btrfs-snapshot-dedup

| | btrfs-snapshot-dedup | btrfs-snapshot-compress |
|---|---|---|
| Question | "Are these N extents identical and should they share?" | "Is this extent compressible? If yes, propagate to sharers." |
| Scan unit | File (with snapshot lookup) | Extent (with reflink lookup) |
| Reflink discovery | Snapshot-walk (live ↔ snapshots) | `LOGICAL_INO_V2` (any reflink, snapshot or not) |
| Source selection | Picks among candidates | Always the freshly compressed extent |
| Hot path | pread prefetch + byte-compare | Probe-compress + DEFRAG_RANGE |

Both can run on the same volume; order is up to you (compress first to maximize gains, then optional dedup-only pass).

## Requirements

- Linux with btrfs filesystem
- Go 1.22+ with cgo (uses kernel headers for ioctl definitions)
- Root (BTRFS ioctls)
- Kernel ≥ 5.6 recommended (for stable `BTRFS_IOC_LOGICAL_INO_V2` and `DEFRAG_RANGE` semantics)

## Build

```bash
go build -o btrfs-snapshot-compress
```

## Usage

```
sudo ./btrfs-snapshot-compress [options] <mount> <subvol> [find-filter...]
```

Everything after `<mount> <subvol>` is passed to `find(1)` as filter (default: `-type f`).

### Examples

```bash
# Compress everything in mysubvol
sudo ./btrfs-snapshot-compress /mnt/btrfs mysubvol

# Only files >= 4 KiB and <= 1 MiB (typical text/config range)
sudo ./btrfs-snapshot-compress /mnt/btrfs mysubvol -size +4k -size -1M

# Only common compressible extensions
sudo ./btrfs-snapshot-compress /mnt/btrfs mysubvol \
  '(' -iname '*.txt' -o -iname '*.json' -o -iname '*.log' \
   -o -iname '*.html' -o -iname '*.csv' -o -iname '*.md' ')'

# Resume after interrupt
sudo ./btrfs-snapshot-compress -start-at 'path/to/last/file' /mnt/btrfs mysubvol

# Stay on the start subvol only (skip nested subvols)
sudo ./btrfs-snapshot-compress /mnt/btrfs mysubvol -xdev
```

### Options

| Flag | Default | Description |
|---|---|---|
| `-workers` | 4 | parallel compress workers |
| `-start-at` | "" | resume marker — skip files until this relative path |
| `-debug` | 0 | log actions taking ≥ N ms to debug.log |
| `-min-size` | 4096 | skip files smaller than this many bytes |
| `-probe-ratio` | 1.20 | minimum compression ratio to actually compress (otherwise skip) |
| `-skip-incompressible-ext` | true | skip known-incompressible extensions (mp4, jpg, zip, …) without probing |

## Status output

Every 10s on stderr:

```
[1m23s] found=12345 buf=234 checked=8000/3210/45/12 probed=8000 skipped=2100/180/30 compressed=2880 reflinks=4521 saved=412.3M ETA:5m23s
```

Field meanings:
- `found` — files matched by walker so far
- `buf` — files in queue waiting
- `checked` — processed/incompressible/notFound/changed
- `probed` — files where probe-compress was run
- `skipped` — extByBlacklist / belowMinSize / nocow
- `compressed` — extents successfully recompressed
- `reflinks` — FIDEDUPERANGE successes for sibling extents
- `saved` — bytes freed (uncompressed minus compressed, after reflink propagation)

## Algorithm details

See [the design doc in kdb](https://github.com/Finomosec/btrfs-snapshot-compress/blob/main/docs/algorithm.md) (mirrored from `plans/btrfs-smart-compress-dedup.md` of the personal kdb).

Key differences from naive `defragment -czstd`:

1. **Probe-compress first** — read first 128 KB, zstd-compress in-memory, only proceed if ratio ≥ threshold (default 1.20×). Skips bulk of mediafiles cheaply.
2. **NOCOW / inline / blacklisted-ext skip** — files with `chattr +C`, files smaller than min-size, files with extension in skip-list (mp4/jpg/zip/…) are not probed at all.
3. **DEFRAG_RANGE per extent**, not whole file — finer atomicity, less risk on near-full disks.
4. **LOGICAL_INO_V2 after compress** — find every file referencing the old physical address.
5. **FIDEDUPERANGE on all siblings** — pull references to the new compressed extent. Kernel verifies bytes (no race conditions). Read-only snapshots work via `O_RDONLY` dest-fd trick.

## Safety / `btrfs send` / RO snapshots

- FIDEDUPERANGE preserves `btrfs send` compatibility.
- Read-only snapshots are handled via `O_RDONLY` dest-fd — no need to flip `ro` property.
- A faster alternative using `BTRFS_IOC_CLONE_RANGE` exists but breaks `btrfs send` and has race-condition risks. Not implemented in this version. May land later behind an explicit `--dangerous-fast-clone` flag.

## License

Same as btrfs-snapshot-dedup (MIT or whatever the dedup project uses).

## Related

- [Finomosec/btrfs-snapshot-dedup](https://github.com/Finomosec/btrfs-snapshot-dedup)
- bees: https://github.com/Zygo/bees — continuous block-hash-based dedup daemon. We share the prefetch trick (pread brute-force) but our scan model is different.
