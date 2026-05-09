# btrfs-snapshot-compress

Apply zstd compression to existing btrfs data **transparently** — without breaking
snapshots, reflinks, or `btrfs send` incremental chains, and without the temporary
disk-usage explosion that plain `btrfs filesystem defragment -czstd` causes on
volumes with shared extents.

Sister tool to [btrfs-snapshot-dedup](https://github.com/Finomosec/btrfs-snapshot-dedup).
Same parameter style, same find-filter logic, same status conventions.

## Why not just `btrfs filesystem defragment -czstd`?

Plain defrag-with-compression rewrites each extent into a new compressed extent.
But snapshots, `cp --reflink` copies, and prior `duperemove` output keep
referencing the **old** uncompressed extent — so:

1. The old extent stays alive (still referenced)
2. The new compressed extent is added to disk
3. **Disk usage grows** instead of shrinking — by exactly the compressed size
4. Net result on a heavily-shared volume: `+(N-1)/N × compressed_size`

For a volume with 39 % sharing (typical after a `duperemove` run), this can fill a
near-full disk before any compression benefit becomes visible. On a `btrfs send`
target, it also breaks the incremental chain.

This tool fixes that by retargeting **every** reflink sibling onto the new
compressed extent atomically per-extent, before moving on. The old extent
becomes orphan, the kernel frees it, sharing is preserved with the compressed
data, and `btrfs send` keeps working.

## Advantages

| Aspect | `btrfs filesystem defragment -czstd` | btrfs-snapshot-compress |
|---|---|---|
| Snapshots / reflinks | **Breaks sharing** → disk grows | **Preserves sharing** → disk shrinks |
| `btrfs send` chain | Fine | Fine (FIDEDUPERANGE preserves it) |
| Already-compressed extents | Skips | Skips |
| Files that don't compress well | Wastes I/O rewriting them anyway | **Probe-compress** bails out cheaply |
| Per-extension learning | n/a | **Smart-Speed** locks in skip/fastpath after sample |
| I/O on uncompressible content | High | **Low** (probe is 128 KiB read, no write) |
| Status reporting | None | 10 s status with file/extent/reflink counters |

The probe-compress + smart-speed combination means the tool only touches files
that **actually benefit** — so unchanged files mean less I/O, less metadata
churn, less follow-up work for backups, snapshots, replication, or any other
system that watches your volume.

## How It Works

For each file from `find(1)`:

1. **Probe-compress** — read first 128 KiB, zstd-encode in memory.
   If ratio < threshold (default 1.20×) → bail out, no writes.
2. **For each extent in the file (FIEMAP):**
   1. Skip if already encoded / inline / unwritten / preallocated
   2. **`BTRFS_IOC_DEFRAG_RANGE`** with `COMPRESS|ZSTD` on the live extent
   3. **`BTRFS_IOC_LOGICAL_INO_V2`** on the old physical address →
      list every reflink sibling (snapshots, `cp --reflink`, prior duperemove output)
   4. Sibling path resolution via **`BTRFS_IOC_INO_PATHS`** + cached subvol-fd
   5. For each sibling: pread-prefetch + **`FIDEDUPERANGE`** retargets it onto
      the new compressed extent
   6. Old extent is now orphan → btrfs cleaner frees it on next commit

The "danger window" is bounded by a single extent (typically ≤ 128 MB), not a
whole file. Atomic per-extent — compressions never accumulate without their
reflink propagation.

## Smart-Speed (always on)

Per-extension learning, case-insensitive. Always active — the default
behaviour. Real-world workloads have shown 80 %+ of files reach the fastpath
after a few thousand probes, drastically cutting probe-I/O.

- After **20 samples** per extension (configurable via `-smart-min-samples`):
  - **< 10 % compressible** → skip the whole extension
  - **≥ 90 % compressible** → fastpath (compress without probing)
- **1-in-50 resample** for drift detection (`-smart-resample`)
- Files without an extension are always probed

The final summary lists the top observed extensions with their compressible-rate.
On real-world data we've observed binaries (`.dll .so .exe`) at 95–100 %
compressible — counter-intuitive but solid — and `.beam` (Erlang bytecode) /
`.rpgmvp` (RPG Maker encrypted PNG) / `.pdf` at the low end.

A static blacklist of always-skipped extensions (`mp4 jpg jpeg png webp mov mp3
m4a flac zip gz bz2 xz zst 7z rar iso ...`) runs before smart-speed and can be
disabled via `-skip-incompressible-ext=false`.

### Thorough mode — never skip an extension

If you want every file at least *evaluated*, without smart-speed locking
whole extensions out as "almost always incompressible":

```bash
sudo ./btrfs-snapshot-compress -smart-skip-thresh=0 /mnt/btrfs mysubvol
```

`-smart-skip-thresh=0` means "skip extension only if < 0 % compressible" — a
threshold that's never reached, so no extension is ever skipped wholesale.
Every file goes through either probe-then-compress or fastpath-compress.
Fastpath stays active and is normally what you want: it skips the probe-read
for extensions with > 95 % success rate (`.txt`, `.js`, etc.) but still
compresses the file. No thoroughness lost — just probe-I/O saved on data
that practically always compresses.

This is the recommended thorough setting.

### Strict mode — probe every file individually

Rarely needed. In strict mode every individual file is probed, even those
in extensions that have been observed at 100 % compressible. The decision
to compress is based on *that file's own* probe, not on the extension's
historical rate. Edge cases this catches: a `.txt` that happens to contain
random data, etc.

```bash
sudo ./btrfs-snapshot-compress -smart-skip-thresh=0 -smart-fast-thresh=2.0 /mnt/btrfs mysubvol
```

`-smart-fast-thresh=2.0` is unreachable since rates are in [0, 1] — fastpath
never triggers, every file is probed. Costs more I/O and CPU than the default
or thorough mode; only useful for forensic / pathological scenarios.

## Requirements

- Linux with btrfs filesystem
- Go 1.22+ with cgo (uses kernel headers for ioctl definitions)
- Kernel ≥ 5.6 recommended (stable `BTRFS_IOC_LOGICAL_INO_V2` semantics)
- Root access (BTRFS ioctls)
- `btrfs` cli in PATH (subprocess call for `subvolid-resolve`)

## Build

```bash
go build -o btrfs-snapshot-compress
```

## Usage

```bash
sudo ./btrfs-snapshot-compress [options] <mountpoint> <subvolume> [find-filter...]
```

Everything after `<mountpoint> <subvolume>` is passed directly to `find(1)` as
filter arguments. Default (no filter): `find <path> -type f`.

`<subvolume>` can be `.` to scan the entire mount.

### Important: top-level subvolume must be reachable

Reflink siblings can live in any subvolume on the filesystem. The tool resolves
sibling paths via `btrfs inspect-internal subvolid-resolve <id> <mount>` —
which returns paths **relative to the FS top-level (subvolid=5)**.

This means the mount you pass must either be the top-level itself, or it must
expose all needed subvols as subdirectories. Practical options:

- **Mount the top-level** somewhere accessible:
  `sudo mount -o subvolid=5 /dev/your-luks-or-block-device /mnt/btrfs-top`
  then run `sudo ./btrfs-snapshot-compress / mnt/btrfs-top .`
- **Run on `/`** if your root is mounted with `subvolid=5` (some installs).
- **Single-subvol-only mounts** (e.g. just `/home` with `subvolid=258`) are
  not enough on their own — sibling resolution will fail for refs in other
  subvols.

If sibling resolution fails frequently, the `path-resolve` error counter in
the status output will grow. Compression of the live file still works — only
the reflink propagation fails for unreachable siblings.

### Lowering system impact (recommended for system SSDs)

Compress + reflink propagation generates substantial I/O. On a busy system
SSD the foreground latency can suffer noticeably. Mitigations:

```bash
sudo nice -n 19 ionice -c 3 ./btrfs-snapshot-compress -workers 1 /mnt/btrfs mysubvol
```

- `nice -n 19` — lowest CPU priority
- `ionice -c 3` — idle I/O class (only when nothing else needs disk)
- `-workers 1` — process one file at a time

`-workers 1` alone gives most of the win on heavily-loaded systems. Total
runtime grows roughly linearly, but the desktop stays responsive.

### Examples

```bash
# Compress everything in mysubvol
sudo ./btrfs-snapshot-compress /mnt/btrfs mysubvol

# Conservative first run on a near-full disk: tiny files only, single worker
sudo ./btrfs-snapshot-compress -workers 1 \
  -size +4k -size -32k /mnt/btrfs mysubvol

# Only common compressible extensions
sudo ./btrfs-snapshot-compress /mnt/btrfs mysubvol \
  '(' -iname '*.txt' -o -iname '*.json' -o -iname '*.log' \
   -o -iname '*.html' -o -iname '*.csv' -o -iname '*.md' ')'

# Resume after interrupt
sudo ./btrfs-snapshot-compress -start-at 'path/to/last/file' /mnt/btrfs mysubvol
```

### Options

| Flag | Default | Description |
|---|---|---|
| `-workers` | `4` | Parallel workers (each processes one file at a time) |
| `-start-at` | `""` | Resume: skip files until this relative path (lexicographic) |
| `-debug` | `0` | Log actions taking ≥ N ms to `debug.log` (0 = off) |
| `-min-size` | `4096` | Skip files smaller than this many bytes |
| `-probe-ratio` | `1.20` | Minimum compression ratio to actually compress (else skip file) |
| `-skip-incompressible-ext` | `true` | Skip known-incompressible extensions without probing |
| `-smart-min-samples` | `20` | Smart-speed: probes before locking in a decision |
| `-smart-skip-thresh` | `0.10` | Smart-speed: < X compressible-rate → skip whole extension |
| `-smart-fast-thresh` | `0.90` | Smart-speed: ≥ X compressible-rate → skip probe (fastpath) |
| `-smart-resample` | `50` | Smart-speed: probe 1-in-N files even after lock-in |

## Output

### Live status (stderr, every 10 seconds)

```
[1m23s] found=12345 buf=234 checked=8000/3210/45/12 skip=4594/28832/0/8694/137 \
        probed=12653(fast=3723) poor=7977 cmp=1180 refs=945/17760/0/1 saved=297M ETA:5m23s
```

| Field | Meaning |
|---|---|
| `found` | Files matched by walker so far |
| `buf` | Files queued waiting |
| `checked` | processed/incompressible/notFound/changed |
| `skip` | ext-blacklist / below-min-size / nocow / already-compressed / smart-skip |
| `probed` | files where probe-compress was run (`fast=N` bypassed by smart-speed) |
| `poor` | files skipped because probe ratio was below threshold |
| `cmp` | extents successfully recompressed |
| `refs` | filesWithSiblings / dedupSucc / dedupFail / lookupErr (reflink propagation) |
| `saved` | approximate on-disk bytes freed (under-counts; `df` is the truth) |

### Final summary

After completion, prints aggregate counters and a top-N table of extensions
with their observed compressible-rate. Useful both as a sanity-check and as
a guide for tuning future runs.

### Output files

- **`debug.log`** — detailed timing log (only with `-debug`)
- **`btrfs-snapshot-compress.log`** — per-file action log
- The first 5 errors of each reflink-resolution phase (`path-resolve` / `open` /
  `ioctl`) are printed to stderr with errno detail; further occurrences
  suppressed.

On interrupt (Ctrl+C), the tool stops cleanly after the current file. Resume
via `-start-at <last-file-path>`.

## Operational Notes

### Disk-usage during operation

Per-extent atomic compress + reflink-propagation means transient overhead is
small (one extent at a time per worker). Still, on near-full volumes:

- Watch `df -h /your-mount` and `btrfs filesystem usage /your-mount`
- Start small (size filter, single worker) and scale up after the first run
  shows healthy `df` behaviour
- The reported `saved` counter under-counts (see Reporting Limits below). `df`
  is your real source of truth.

### Reporting limits

The `saved` counter currently reports approximate boundary deltas, not real
on-disk bytes freed. The actual disk savings come from two sources we don't
track precisely yet:

1. **Compression itself** — the new compressed extent's `disk_num_bytes` vs
   the old uncompressed `disk_num_bytes`. FIEMAP returns logical (uncompressed)
   sizes for compressed extents, so this is invisible to a FIEMAP-only walk.
   `BTRFS_IOC_TREE_SEARCH_V2` on the extent tree can give the real value;
   not yet implemented in this tool.
2. **Reflink-orphan freeing** — when all siblings of an old extent are
   redirected, the kernel cleaner frees the entire old extent on next commit.

For real measurements use `btrfs filesystem usage`, `compsize`, or compare
`df -h` before/after.

### Safety

The tool only writes via two well-tested kernel ioctls:

- **`BTRFS_IOC_DEFRAG_RANGE`** — same ioctl `btrfs filesystem defragment` uses.
  Atomic per extent. File logical content is **byte-identical** before and
  after — only the on-disk encoding changes.
- **`FIDEDUPERANGE`** — kernel performs a **byte-by-byte verification** of
  src vs dst before retargeting any extent. Cannot corrupt data.

To verify after a run: `git status` on git-tracked subvols, `sha256sum`
comparisons against backups, or `btrfs scrub start -B` for full filesystem
checksum verification.

## Combine with btrfs-snapshot-dedup

Because compress and dedup are different operations, they compose cleanly:

1. Run `btrfs-snapshot-compress` first → compressed live extents,
   reflink-sharing preserved
2. Optional: run `btrfs-snapshot-dedup` second → finds byte-identical files
   that aren't yet reflink-shared (e.g. across subvols) and unifies them

The reverse order also works.

## Technical Notes

### Why pread instead of fadvise / readahead?

`readahead()` is capped by `read_ahead_kb` (typically 128 KiB). `posix_fadvise
(FADV_WILLNEED)` has no cap but btrfs runs it at low I/O priority and may
discard it. Brute-force `pread()` into a dummy buffer is the only reliable way
to warm the page cache before `FIDEDUPERANGE`. Without it, throughput drops
roughly 10×. This is the same lesson [bees](https://github.com/Zygo/bees)
documented years ago.

### Why INO_PATHS instead of INO_LOOKUP?

`BTRFS_IOC_INO_LOOKUP` only finds `inode_ref` entries. Files in heavily
populated directories (e.g. `.git/lfs/objects/<2>/<2>/`) use `inode_extref`
(extended refs) — INO_LOOKUP returns ENOENT for them. `BTRFS_IOC_INO_PATHS`
handles both ref types but uses the file descriptor's tree, not an explicit
treeid. We open and cache one fd per subvol root.

### Why FIDEDUPERANGE instead of CLONE_RANGE?

`BTRFS_IOC_CLONE_RANGE` would be much faster (pointer manipulation, no byte
compare) but:
- breaks `btrfs send` incremental chains
- introduces race conditions (no kernel verify)
- requires write-mode access (no on read-only snapshots)

`FIDEDUPERANGE`'s byte-verify is slower per call but completely safe and
preserves `btrfs send` compatibility. With pread-prefetch the throughput is
~500 MB/min, which is fine for the use case.

### Path resolution mechanics

Per-subvol fd cache (opened lazily, kept for program lifetime). Subvol path
resolved via `btrfs inspect-internal subvolid-resolve` subprocess (cached).
INO_PATHS returns paths relative to subvol root; we prepend the subvol's
path-from-mount to get the absolute path.

## Related

- [bees](https://github.com/Zygo/bees) — Continuous block-hash-based btrfs
  dedup daemon. We share the pread-prefetch trick, but the scan model is
  different (file-walk vs extent-tree-scan).
- [btrfs-snapshot-dedup](https://github.com/Finomosec/btrfs-snapshot-dedup) —
  Sister project: deduplicate broken snapshot extents.
- [duperemove](https://github.com/markfasheh/duperemove) — Hash-based dedup
  tool. After running compress, you can run duperemove to find further dedup
  candidates not yet reflink-shared.

## License

GPL-2.0 (same as the dedup sister project).
