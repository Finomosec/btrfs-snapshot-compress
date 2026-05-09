// btrfs-snapshot-compress — compress btrfs extents with reflink-preservation.
//
// For each live extent we:
//   1. Probe-compress the first 128 KiB → bail out if the ratio is poor.
//   2. LOGICAL_INO_V2(phys) → list every reflink sibling.
//   3. BTRFS_IOC_DEFRAG_RANGE (COMPRESS|ZSTD) on the live extent.
//   4. FIDEDUPERANGE for every sibling → pull references to the new compressed extent.
//   5. Old uncompressed extent becomes orphan → kernel frees it.
//
// Sister project of btrfs-snapshot-dedup. Same parameter style, same find-filter
// logic, same SpillQueue/Worker-Pool pattern.
package main

/*
#include <linux/btrfs.h>
#include <linux/btrfs_tree.h>
#include <linux/fs.h>
#include <linux/fiemap.h>
#include <stddef.h>

const unsigned long IOCTL_FIEMAP            = FS_IOC_FIEMAP;
const unsigned long IOCTL_FIDEDUPERANGE     = FIDEDUPERANGE;
const unsigned long IOCTL_LOGICAL_INO_V2    = BTRFS_IOC_LOGICAL_INO_V2;
const unsigned long IOCTL_DEFRAG_RANGE      = BTRFS_IOC_DEFRAG_RANGE;
const unsigned long IOCTL_INO_LOOKUP        = BTRFS_IOC_INO_LOOKUP;

const int DEDUPE_RANGE_SIZE        = sizeof(struct file_dedupe_range);
const int DEDUPE_RANGE_INFO_SIZE   = sizeof(struct file_dedupe_range_info);
const int DEFRAG_RANGE_ARGS_SIZE   = sizeof(struct btrfs_ioctl_defrag_range_args);
*/
import "C"

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"

	"github.com/klauspost/compress/zstd"
)

var (
	IOC_FIEMAP         = uintptr(C.IOCTL_FIEMAP)
	IOC_FIDEDUPERANGE  = uintptr(C.IOCTL_FIDEDUPERANGE)
	IOC_LOGICAL_INO_V2 = uintptr(C.IOCTL_LOGICAL_INO_V2)
	IOC_DEFRAG_RANGE   = uintptr(C.IOCTL_DEFRAG_RANGE)
	IOC_INO_LOOKUP     = uintptr(C.IOCTL_INO_LOOKUP)

	DEDUPE_RANGE_SIZE      = int(C.DEDUPE_RANGE_SIZE)
	DEDUPE_RANGE_INFO_SIZE = int(C.DEDUPE_RANGE_INFO_SIZE)
	DEFRAG_RANGE_ARGS_SIZE = int(C.DEFRAG_RANGE_ARGS_SIZE)
)

const VERSION = "0.1.0"

const (
	QUEUE_LIMIT       = 10000
	DEFAULT_WORKERS   = 4
	PROBE_BYTES       = 128 * 1024 // bytes read for compressibility probe
	PREFETCH_CHUNK    = 128 * 1024
	FIEMAP_HEADER     = C.sizeof_struct_fiemap
	FIEMAP_EXTENT_LEN = C.sizeof_struct_fiemap_extent

	// FIEMAP extent flags
	FIEMAP_EXTENT_LAST       = 0x00000001
	FIEMAP_EXTENT_ENCODED    = 0x00000008 // compressed
	FIEMAP_EXTENT_UNKNOWN    = 0x00000002
	FIEMAP_EXTENT_DELALLOC   = 0x00000004
	FIEMAP_EXTENT_NOT_ALIGNED = 0x00000100
	FIEMAP_EXTENT_DATA_INLINE = 0x00000200
	FIEMAP_EXTENT_DATA_TAIL  = 0x00000400
	FIEMAP_EXTENT_UNWRITTEN  = 0x00000800
	FIEMAP_EXTENT_MERGED     = 0x00001000
	FIEMAP_EXTENT_SHARED     = 0x00002000

	// btrfs_ioctl_defrag_range_args.flags
	BTRFS_DEFRAG_RANGE_COMPRESS  = 1
	BTRFS_DEFRAG_RANGE_START_IO  = 2

	// btrfs compress types
	BTRFS_COMPRESS_NONE = 0
	BTRFS_COMPRESS_ZLIB = 1
	BTRFS_COMPRESS_LZO  = 2
	BTRFS_COMPRESS_ZSTD = 3

	// FS NOCOW flag (chattr +C)
	FS_NOCOW_FL = 0x00800000
	FS_IOC_GETFLAGS = 0x80086601
)

// Default extension blacklist — files of these types are skipped without probing.
// All lower-case, matched against suffix.
var DEFAULT_INCOMPRESSIBLE_EXTS = []string{
	// Pre-compressed images
	"jpg", "jpeg", "png", "webp", "heic", "heif", "avif", "gif",
	// Pre-compressed video
	"mp4", "mkv", "webm", "avi", "mov", "m4v", "wmv", "flv", "mpg", "mpeg",
	// Pre-compressed audio
	"mp3", "m4a", "ogg", "opus", "flac", "wma", "aac", "aiff",
	// Archives
	"zip", "gz", "bz2", "xz", "zst", "7z", "rar", "tgz", "tbz2", "txz",
	"jar", "war", "ear", "apk", "ipa", "xpi",
	// Disk images
	"iso", "img", "qcow2", "vhdx", "vmdk", "squashfs",
	// Office (often zip-based)
	"docx", "xlsx", "pptx", "odt", "ods", "odp", "epub",
	// Crypto/signed
	"deb", "rpm", "snap",
}

// =================================================================
// SpillQueue — same pattern as btrfs-snapshot-dedup
// =================================================================

type SpillQueue struct {
	mu        sync.Mutex
	queue     []string
	limit     int
	spillFile *os.File
	spillW    *bufio.Writer
	spillPath string
	spillR    *bufio.Scanner
	spilled   int64
	refilled  int64
	total     atomic.Int64
	closed    bool
}

func NewSpillQueue(limit int) *SpillQueue {
	return &SpillQueue{queue: make([]string, 0, limit), limit: limit}
}

func (q *SpillQueue) Push(s string) {
	q.total.Add(1)
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.queue) < q.limit {
		q.queue = append(q.queue, s)
		return
	}
	if q.spillFile == nil {
		f, err := os.CreateTemp("", "compress-spill-*.txt")
		if err != nil {
			q.queue = append(q.queue, s)
			return
		}
		q.spillFile = f
		q.spillW = bufio.NewWriter(f)
		q.spillPath = f.Name()
	}
	fmt.Fprintln(q.spillW, s)
	q.spilled++
}

func (q *SpillQueue) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.closed = true
	if q.spillW != nil {
		q.spillW.Flush()
	}
}

func (q *SpillQueue) Pop() (string, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.queue) > 0 {
		s := q.queue[0]
		q.queue = q.queue[1:]
		return s, true
	}
	if q.spillR == nil && q.spillFile != nil {
		q.spillW.Flush()
		// reopen for reading
		f, err := os.Open(q.spillPath)
		if err == nil {
			q.spillR = bufio.NewScanner(f)
			q.spillR.Buffer(make([]byte, 1024*1024), 1024*1024)
		}
	}
	if q.spillR != nil && q.spillR.Scan() {
		q.refilled++
		return q.spillR.Text(), true
	}
	if q.closed {
		return "", false
	}
	return "", false
}

func (q *SpillQueue) Total() int64    { return q.total.Load() }
func (q *SpillQueue) Buffered() int   { q.mu.Lock(); defer q.mu.Unlock(); return len(q.queue) }
func (q *SpillQueue) Cleanup() {
	if q.spillFile != nil {
		q.spillFile.Close()
		os.Remove(q.spillPath)
	}
}

// =================================================================
// Format helpers
// =================================================================

func fmtTime(seconds int) string {
	if seconds < 60 {
		return fmt.Sprintf("%ds", seconds)
	} else if seconds < 3600 {
		return fmt.Sprintf("%dm%02ds", seconds/60, seconds%60)
	}
	return fmt.Sprintf("%dh%02dm", seconds/3600, (seconds%3600)/60)
}

func fmtBytes(b int64) string {
	if b >= 1024*1024*1024 {
		return fmt.Sprintf("%.1fG", float64(b)/(1024*1024*1024))
	} else if b >= 1024*1024 {
		return fmt.Sprintf("%.1fM", float64(b)/(1024*1024))
	} else if b >= 1024 {
		return fmt.Sprintf("%.1fK", float64(b)/1024)
	}
	return fmt.Sprintf("%dB", b)
}

// =================================================================
// FIEMAP wrapper
// =================================================================

type extent struct {
	logical  uint64
	physical uint64
	length   uint64
	flags    uint32
}

// fiemap returns the extents of an fd. limit caps the count.
func fiemap(fd int, limit int) ([]extent, error) {
	if limit < 1 {
		limit = 256
	}
	bufSize := FIEMAP_HEADER + limit*FIEMAP_EXTENT_LEN
	buf := make([]byte, bufSize)
	// struct fiemap header
	binary.LittleEndian.PutUint64(buf[0:8], 0)                // fm_start
	binary.LittleEndian.PutUint64(buf[8:16], ^uint64(0))      // fm_length (max)
	binary.LittleEndian.PutUint32(buf[16:20], 0)              // fm_flags
	binary.LittleEndian.PutUint32(buf[20:24], 0)              // fm_mapped_extents (out)
	binary.LittleEndian.PutUint32(buf[24:28], uint32(limit))  // fm_extent_count
	binary.LittleEndian.PutUint32(buf[28:32], 0)              // reserved

	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, uintptr(fd), IOC_FIEMAP, uintptr(unsafe.Pointer(&buf[0])))
	if errno != 0 {
		return nil, errno
	}
	mapped := binary.LittleEndian.Uint32(buf[20:24])
	if mapped > uint32(limit) {
		mapped = uint32(limit)
	}
	out := make([]extent, mapped)
	for i := uint32(0); i < mapped; i++ {
		off := FIEMAP_HEADER + int(i)*FIEMAP_EXTENT_LEN
		out[i] = extent{
			logical:  binary.LittleEndian.Uint64(buf[off : off+8]),
			physical: binary.LittleEndian.Uint64(buf[off+8 : off+16]),
			length:   binary.LittleEndian.Uint64(buf[off+16 : off+24]),
			flags:    binary.LittleEndian.Uint32(buf[off+40 : off+44]),
		}
	}
	return out, nil
}

// =================================================================
// LOGICAL_INO_V2 — find all files referencing a physical address
// =================================================================

type reflinkRef struct {
	inum   uint64
	offset uint64
	root   uint64
}

// logicalResolve resolves a physical address (logical in btrfs terminology) to
// all (root, inode, offset) tuples referencing it.
func logicalResolve(mountFd int, phys uint64) ([]reflinkRef, error) {
	// btrfs_ioctl_logical_ino_args (V2 supports flags + size override)
	const argsSize = 56
	const flagIgnoreOffset = 1
	bufSize := 65536
	buf := make([]byte, bufSize)

	args := make([]byte, argsSize)
	binary.LittleEndian.PutUint64(args[0:8], phys)             // logical
	binary.LittleEndian.PutUint64(args[8:16], uint64(bufSize)) // size
	// reserved [3]u64 at args[16:40] (V2 specific: index [3] is flags)
	binary.LittleEndian.PutUint64(args[40:48], flagIgnoreOffset) // flags
	binary.LittleEndian.PutUint64(args[48:56], uint64(uintptr(unsafe.Pointer(&buf[0])))) // inodes ptr

	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, uintptr(mountFd), IOC_LOGICAL_INO_V2, uintptr(unsafe.Pointer(&args[0])))
	if errno != 0 {
		return nil, errno
	}
	// Result: btrfs_data_container header (32 bytes) + array of u64 triples (inum, offset, root)
	if len(buf) < 32 {
		return nil, nil
	}
	bytesLeft := binary.LittleEndian.Uint32(buf[0:4])
	elemCnt := binary.LittleEndian.Uint32(buf[4:8])
	elemMissed := binary.LittleEndian.Uint32(buf[8:12])
	_ = bytesLeft
	_ = elemMissed
	// Triples count: elemCnt counts total u64s, divide by 3
	tripleCnt := int(elemCnt) / 3
	out := make([]reflinkRef, 0, tripleCnt)
	for i := 0; i < tripleCnt; i++ {
		off := 32 + i*24
		if off+24 > len(buf) {
			break
		}
		out = append(out, reflinkRef{
			inum:   binary.LittleEndian.Uint64(buf[off : off+8]),
			offset: binary.LittleEndian.Uint64(buf[off+8 : off+16]),
			root:   binary.LittleEndian.Uint64(buf[off+16 : off+24]),
		})
	}
	return out, nil
}

// =================================================================
// DEFRAG_RANGE wrapper
// =================================================================

func defragRangeCompressZSTD(fd int, start, length uint64) error {
	args := make([]byte, DEFRAG_RANGE_ARGS_SIZE)
	binary.LittleEndian.PutUint64(args[0:8], start)
	binary.LittleEndian.PutUint64(args[8:16], length)
	binary.LittleEndian.PutUint64(args[16:24], BTRFS_DEFRAG_RANGE_COMPRESS|BTRFS_DEFRAG_RANGE_START_IO)
	binary.LittleEndian.PutUint32(args[24:28], 0)                    // extent_thresh = 0 → no threshold
	binary.LittleEndian.PutUint32(args[28:32], BTRFS_COMPRESS_ZSTD)  // compress_type
	// args[32:48] = unused[4]

	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, uintptr(fd), IOC_DEFRAG_RANGE, uintptr(unsafe.Pointer(&args[0])))
	if errno != 0 {
		return errno
	}
	return nil
}

// =================================================================
// Probe-compress
// =================================================================

var probeEncoder, _ = zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))

// probeCompressible reads up to PROBE_BYTES from the file at the given offset
// and returns the achieved compression ratio (uncompressed / compressed).
// A ratio < 1 means compression made it bigger; >1 means smaller.
func probeCompressible(fd int, offset int64, length int64) (ratio float64, err error) {
	probeLen := int64(PROBE_BYTES)
	if length < probeLen {
		probeLen = length
	}
	if probeLen <= 0 {
		return 0, fmt.Errorf("zero length")
	}
	buf := make([]byte, probeLen)
	n, err := syscall.Pread(fd, buf, offset)
	if err != nil {
		return 0, err
	}
	if n <= 0 {
		return 0, fmt.Errorf("empty read")
	}
	src := buf[:n]
	dst := probeEncoder.EncodeAll(src, make([]byte, 0, n))
	if len(dst) == 0 {
		return 0, fmt.Errorf("empty encode")
	}
	return float64(len(src)) / float64(len(dst)), nil
}

// =================================================================
// FIDEDUPERANGE
// =================================================================

// fideduperange runs FIDEDUPERANGE for src→dst at offset 0..length.
// Returns bytes_deduped reported by the kernel.
func fideduperange(srcFd int, srcOffset uint64, dstFd int, dstOffset uint64, length uint64) (uint64, error) {
	bufLen := DEDUPE_RANGE_SIZE + DEDUPE_RANGE_INFO_SIZE
	buf := make([]byte, bufLen)
	// struct file_dedupe_range
	binary.LittleEndian.PutUint64(buf[0:8], srcOffset)
	binary.LittleEndian.PutUint64(buf[8:16], length)
	binary.LittleEndian.PutUint16(buf[16:18], 1) // dest_count
	// reserved bytes 18..24
	// struct file_dedupe_range_info
	off := DEDUPE_RANGE_SIZE
	binary.LittleEndian.PutUint64(buf[off:off+8], uint64(dstFd))
	binary.LittleEndian.PutUint64(buf[off+8:off+16], dstOffset)
	binary.LittleEndian.PutUint64(buf[off+16:off+24], 0) // bytes_deduped (out)
	binary.LittleEndian.PutUint32(buf[off+24:off+28], 0) // status (out)
	binary.LittleEndian.PutUint32(buf[off+28:off+32], 0) // reserved

	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, uintptr(srcFd), IOC_FIDEDUPERANGE, uintptr(unsafe.Pointer(&buf[0])))
	if errno != 0 {
		return 0, errno
	}
	bytesDeduped := binary.LittleEndian.Uint64(buf[off+16 : off+24])
	status := int32(binary.LittleEndian.Uint32(buf[off+24 : off+28]))
	if status < 0 {
		return bytesDeduped, fmt.Errorf("dedupe status=%d", status)
	}
	return bytesDeduped, nil
}

// =================================================================
// pread prefetch (force pages into cache, ignore content)
// =================================================================

func prefetchRange(fd int, offset, length int64) {
	dummy := make([]byte, PREFETCH_CHUNK)
	end := offset + length
	for off := offset; off < end; off += PREFETCH_CHUNK {
		l := PREFETCH_CHUNK
		if off+int64(l) > end {
			l = int(end - off)
		}
		syscall.Pread(fd, dummy[:l], off)
	}
}

// =================================================================
// NOCOW check via FS_IOC_GETFLAGS
// =================================================================

func isNocow(fd int) bool {
	var flags uint32
	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, uintptr(fd), FS_IOC_GETFLAGS, uintptr(unsafe.Pointer(&flags)))
	if errno != 0 {
		return false
	}
	return flags&FS_NOCOW_FL != 0
}

// =================================================================
// Resolve (root, inum, offset) → absolute path
// =================================================================

// rootInumToPath uses BTRFS_IOC_INO_LOOKUP to map a root + inum to a path.
// The mountFd is the open fd of the btrfs mount point.
// Returns absolute path under mount.
func rootInumToPath(mount string, mountFd int, root, inum uint64) (string, error) {
	// btrfs_ioctl_ino_lookup_args
	const argsSize = 4096
	args := make([]byte, argsSize)
	binary.LittleEndian.PutUint64(args[0:8], root)
	binary.LittleEndian.PutUint64(args[8:16], inum)
	// 4080 bytes of name buffer follow

	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, uintptr(mountFd), IOC_INO_LOOKUP, uintptr(unsafe.Pointer(&args[0])))
	if errno != 0 {
		return "", errno
	}
	// args[16:] is the directory path (NUL-terminated). For inum-resolved files
	// btrfs returns the path-to-the-CONTAINING-directory but not the file name.
	// We then need to scan that directory for inum match.
	dirRel := strings.TrimRight(string(bytes.TrimRight(args[16:], "\x00")), "/")
	// Resolve subvol root path via /proc — fallback: scan
	subvolRoot, err := resolveSubvolPath(mount, mountFd, root)
	if err != nil {
		return "", err
	}
	dir := subvolRoot
	if dirRel != "" {
		dir = filepath.Join(subvolRoot, dirRel)
	}
	// Scan dir for the inode
	entries, err := os.ReadDir(dir)
	if err != nil {
		return "", err
	}
	for _, e := range entries {
		fi, err := e.Info()
		if err != nil {
			continue
		}
		st, ok := fi.Sys().(*syscall.Stat_t)
		if !ok {
			continue
		}
		if st.Ino == inum {
			return filepath.Join(dir, e.Name()), nil
		}
	}
	return "", fmt.Errorf("inum %d not found in %s", inum, dir)
}

// Cache of root → subvol path (relative to mount). Filled lazily by
// reading the subvol tree via INO_LOOKUP on rootid (inum=0).
var (
	subvolPathCacheMu sync.Mutex
	subvolPathCache   = make(map[uint64]string)
)

func resolveSubvolPath(mount string, mountFd int, root uint64) (string, error) {
	subvolPathCacheMu.Lock()
	if p, ok := subvolPathCache[root]; ok {
		subvolPathCacheMu.Unlock()
		return p, nil
	}
	subvolPathCacheMu.Unlock()

	// INO_LOOKUP with inum=0 returns the subvol's path relative to the FS top.
	const argsSize = 4096
	args := make([]byte, argsSize)
	binary.LittleEndian.PutUint64(args[0:8], root)
	binary.LittleEndian.PutUint64(args[8:16], 0)

	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, uintptr(mountFd), IOC_INO_LOOKUP, uintptr(unsafe.Pointer(&args[0])))
	if errno != 0 {
		return "", errno
	}
	rel := strings.TrimRight(string(bytes.TrimRight(args[16:], "\x00")), "/")
	abs := mount
	if rel != "" {
		abs = filepath.Join(mount, rel)
	}
	subvolPathCacheMu.Lock()
	subvolPathCache[root] = abs
	subvolPathCacheMu.Unlock()
	return abs, nil
}

// =================================================================
// File-level processing
// =================================================================

type counters struct {
	checked         atomic.Int64
	incompressible  atomic.Int64
	notFound        atomic.Int64
	changed         atomic.Int64

	probed          atomic.Int64
	skippedExt      atomic.Int64
	skippedSize     atomic.Int64
	skippedNocow    atomic.Int64
	skippedAlready  atomic.Int64 // file fully already compressed (extent loop early-out)
	skippedSmart    atomic.Int64 // smart-speed decided to skip whole file
	fastpath        atomic.Int64 // smart-speed bypassed the probe (ext is "trusted")
	poorRatio       atomic.Int64

	compressed      atomic.Int64
	reflinks        atomic.Int64
	bytesSaved      atomic.Int64

	pending         atomic.Int64
}

// =================================================================
// Smart-speed: per-extension learning of compressibility
// =================================================================

const (
	SSMODE_PROBE       = 0 // unsure → run probe
	SSMODE_FASTPATH    = 1 // ext mostly compressible → skip probe, just compress
	SSMODE_ALWAYS_SKIP = 2 // ext mostly incompressible → skip whole file
)

type extStats struct {
	mu      sync.Mutex
	total   int
	success int // probe ratio >= threshold
}

type smartSpeed struct {
	enabled    bool
	minSamples int
	skipThresh float64 // <X compressible → ALWAYS_SKIP
	fastThresh float64 // >X compressible → FASTPATH
	resampleN  int     // 1-in-N: force probe even in locked-in mode

	mu    sync.RWMutex
	stats map[string]*extStats
	seen  atomic.Int64 // total decisions made (for resample)
}

func newSmartSpeed(enabled bool, minSamples, resampleN int, skipThresh, fastThresh float64) *smartSpeed {
	return &smartSpeed{
		enabled:    enabled,
		minSamples: minSamples,
		skipThresh: skipThresh,
		fastThresh: fastThresh,
		resampleN:  resampleN,
		stats:      make(map[string]*extStats),
	}
}

// decide returns (doProbe, doProcess) based on ext history.
//   doProbe=true   → run the probe before deciding to compress
//   doProcess=true → process the file at all (false = skip)
// Extensionless files always (true,true). Cold start always (true,true).
func (ss *smartSpeed) decide(ext string) (doProbe, doProcess bool) {
	if !ss.enabled || ext == "" {
		return true, true
	}
	ext = strings.ToLower(ext)
	ss.mu.RLock()
	s, ok := ss.stats[ext]
	ss.mu.RUnlock()
	if !ok {
		return true, true
	}
	s.mu.Lock()
	n := s.total
	succ := s.success
	s.mu.Unlock()
	if n < ss.minSamples {
		return true, true
	}
	rate := float64(succ) / float64(n)

	// Periodic resample: even in locked-in modes, every N-th decision forces a probe
	seen := ss.seen.Add(1)
	if ss.resampleN > 0 && seen%int64(ss.resampleN) == 0 {
		return true, true
	}

	if rate < ss.skipThresh {
		return false, false // skip whole file
	}
	if rate > ss.fastThresh {
		return false, true // skip probe, compress directly
	}
	return true, true
}

// record updates per-ext stats.
func (ss *smartSpeed) record(ext string, isCompressible bool) {
	if !ss.enabled || ext == "" {
		return
	}
	ext = strings.ToLower(ext)
	ss.mu.Lock()
	s, ok := ss.stats[ext]
	if !ok {
		s = &extStats{}
		ss.stats[ext] = s
	}
	ss.mu.Unlock()
	s.mu.Lock()
	s.total++
	if isCompressible {
		s.success++
	}
	s.mu.Unlock()
}

// snapshot returns a copy of the current stats for final reporting.
func (ss *smartSpeed) snapshot() map[string]extStatSnap {
	if !ss.enabled {
		return nil
	}
	out := make(map[string]extStatSnap)
	ss.mu.RLock()
	for k, s := range ss.stats {
		s.mu.Lock()
		out[k] = extStatSnap{total: s.total, success: s.success}
		s.mu.Unlock()
	}
	ss.mu.RUnlock()
	return out
}

type extStatSnap struct {
	total, success int
}

type fileTask struct {
	path string
}

// processFile: walks extents of one file, attempts to compress each.
func processFile(path string, mountFd int, mount string, extBlacklist map[string]bool,
	probeRatioMin float64, minSize int64, ss *smartSpeed, cnt *counters) {

	cnt.pending.Add(1)
	defer cnt.pending.Add(-1)
	defer cnt.checked.Add(1)

	st, err := os.Stat(path)
	if err != nil {
		cnt.notFound.Add(1)
		return
	}
	if !st.Mode().IsRegular() {
		return
	}
	size := st.Size()
	if size < minSize {
		cnt.skippedSize.Add(1)
		return
	}

	// Extension blacklist (hardcoded incompressible types)
	ext := strings.ToLower(strings.TrimPrefix(filepath.Ext(path), "."))
	if ext != "" && extBlacklist[ext] {
		cnt.skippedExt.Add(1)
		return
	}

	// Smart-speed: skip files of extensions known to be incompressible
	doProbe, doProcess := ss.decide(ext)
	if !doProcess {
		cnt.skippedSmart.Add(1)
		return
	}

	fd, err := syscall.Open(path, syscall.O_RDONLY, 0)
	if err != nil {
		cnt.notFound.Add(1)
		return
	}
	defer syscall.Close(fd)

	if isNocow(fd) {
		cnt.skippedNocow.Add(1)
		return
	}

	// Get extents of the file
	exts, err := fiemap(fd, 1024)
	if err != nil || len(exts) == 0 {
		cnt.incompressible.Add(1)
		return
	}

	// Early file-level exit: if every extent is already encoded (compressed),
	// nothing to do — skip without opening RW fd, without probing.
	allEncoded := true
	for _, e := range exts {
		// Inline / unwritten / delalloc count as "no work needed" too
		if e.flags&(FIEMAP_EXTENT_ENCODED|FIEMAP_EXTENT_DATA_INLINE|FIEMAP_EXTENT_UNWRITTEN|FIEMAP_EXTENT_DELALLOC) == 0 {
			allEncoded = false
			break
		}
	}
	if allEncoded {
		cnt.skippedAlready.Add(1)
		return
	}

	// Probe-compress (unless smart-speed says we can trust this extension)
	if doProbe {
		cnt.probed.Add(1)
		ratio, err := probeCompressible(fd, 0, size)
		if err != nil || ratio < probeRatioMin {
			ss.record(ext, false)
			cnt.poorRatio.Add(1)
			return
		}
		ss.record(ext, true)
	} else {
		cnt.fastpath.Add(1)
	}

	// We need a writable fd for DEFRAG_RANGE.
	fdRW, err := syscall.Open(path, syscall.O_RDWR, 0)
	if err != nil {
		cnt.notFound.Add(1)
		return
	}
	defer syscall.Close(fdRW)

	for _, ex := range exts {
		// Skip already-compressed extents
		if ex.flags&FIEMAP_EXTENT_ENCODED != 0 {
			cnt.skippedAlready.Add(1)
			continue
		}
		// Skip inline/preallocated/unwritten
		if ex.flags&(FIEMAP_EXTENT_DATA_INLINE|FIEMAP_EXTENT_UNWRITTEN|FIEMAP_EXTENT_DELALLOC) != 0 {
			cnt.skippedAlready.Add(1)
			continue
		}
		if ex.length == 0 {
			continue
		}

		// Discover reflink siblings for this extent BEFORE we defrag, so we
		// can find them via the old physical address.
		oldPhys := ex.physical
		oldLen := ex.length

		// Prefetch the live extent into cache (helps subsequent FIDEDUPERANGE)
		prefetchRange(fd, int64(ex.logical), int64(ex.length))

		// Defrag-compress this extent in the live file
		if err := defragRangeCompressZSTD(fdRW, ex.logical, ex.length); err != nil {
			cnt.poorRatio.Add(1) // count as failure
			continue
		}

		// Re-FIEMAP just this offset to learn the new physical address & length
		newPhys, newLen, ok := getExtentAt(fdRW, ex.logical)
		if !ok || newLen == 0 || newPhys == oldPhys {
			// Defrag did not actually move the extent — skip
			continue
		}
		cnt.compressed.Add(1)
		// "Saved" estimate: difference between old extent length and new extent length.
		// This is an approximation; the real on-disk savings depend on whether all
		// reflink siblings get pulled to the new extent below.
		if oldLen > newLen {
			cnt.bytesSaved.Add(int64(oldLen - newLen))
		}

		// Reflink propagation: find every other (root, inum, offset) referring
		// to oldPhys → FIDEDUPERANGE that range to point at the new extent.
		refs, err := logicalResolve(mountFd, oldPhys)
		if err != nil || len(refs) == 0 {
			continue
		}

		// Self-inode + root for filtering
		var selfStat syscall.Stat_t
		_ = syscall.Fstat(fdRW, &selfStat)
		selfInum := selfStat.Ino

		for _, r := range refs {
			if r.inum == selfInum {
				continue
			}
			// Resolve sibling path
			sibPath, err := rootInumToPath(mount, mountFd, r.root, r.inum)
			if err != nil {
				continue
			}
			sibFd, err := syscall.Open(sibPath, syscall.O_RDONLY, 0)
			if err != nil {
				continue
			}
			// FIDEDUPERANGE: pull sibling at r.offset, len=newLen, to the live
			// extent at ex.logical with same length.
			useLen := newLen
			if useLen > oldLen {
				useLen = oldLen
			}
			_, derr := fideduperange(fdRW, ex.logical, sibFd, r.offset, useLen)
			syscall.Close(sibFd)
			if derr == nil {
				cnt.reflinks.Add(1)
			}
		}
	}
}

// getExtentAt returns the physical addr + length of the extent that covers the given logical offset.
func getExtentAt(fd int, logical uint64) (uint64, uint64, bool) {
	exts, err := fiemap(fd, 8)
	if err != nil {
		return 0, 0, false
	}
	for _, e := range exts {
		if logical >= e.logical && logical < e.logical+e.length {
			return e.physical, e.length, true
		}
	}
	return 0, 0, false
}

// =================================================================
// Main
// =================================================================

func printUsage() {
	fmt.Fprintf(os.Stderr, "btrfs-snapshot-compress v%s\n\n", VERSION)
	fmt.Fprintf(os.Stderr, "Usage: %s [options] <mount> <subvol> [find-filter...]\n\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "Options:\n")
	flag.PrintDefaults()
	fmt.Fprintf(os.Stderr, "\nEverything after <mount> <subvol> is passed to find(1) as filter.\n")
	fmt.Fprintf(os.Stderr, "Default (no filter): find <path> -type f\n\n")
	fmt.Fprintf(os.Stderr, "Examples:\n")
	fmt.Fprintf(os.Stderr, "  # Compress everything in mysubvol\n")
	fmt.Fprintf(os.Stderr, "  sudo %s /mnt/btrfs mysubvol\n\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "  # Only files between 4 KiB and 1 MiB (typical text/config range)\n")
	fmt.Fprintf(os.Stderr, "  sudo %s /mnt/btrfs mysubvol -size +4k -size -1M\n\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "  # Resume after interrupt\n")
	fmt.Fprintf(os.Stderr, "  sudo %s -start-at 'path/to/last/file' /mnt/btrfs mysubvol\n", os.Args[0])
}

func main() {
	workers := flag.Int("workers", DEFAULT_WORKERS, "number of parallel workers")
	startAt := flag.String("start-at", "", "resume: skip files until this relative path (lexicographic)")
	debugMs := flag.Int("debug", 0, "log actions taking >= N ms to debug.log (0 = off)")
	probeRatioMin := flag.Float64("probe-ratio", 1.20, "minimum compression ratio to actually compress (otherwise skip file)")
	minSize := flag.Int64("min-size", 4096, "skip files smaller than this many bytes")
	skipExt := flag.Bool("skip-incompressible-ext", true, "skip known-incompressible extensions (mp4, jpg, zip, …) without probing")
	smartSpeedFlag := flag.Bool("smart-speed", false, "learn per-extension compressibility and skip extensions that turn out incompressible")
	smartMinSamples := flag.Int("smart-min-samples", 20, "smart-speed: minimum probe samples per extension before locking in")
	smartResample := flag.Int("smart-resample", 50, "smart-speed: probe 1-in-N files even after lock-in (drift detection)")
	smartSkipThresh := flag.Float64("smart-skip-thresh", 0.10, "smart-speed: <X compressible-rate → skip whole extension")
	smartFastThresh := flag.Float64("smart-fast-thresh", 0.95, "smart-speed: >X compressible-rate → skip probe (fastpath)")
	flag.Usage = printUsage
	flag.Parse()

	args := flag.Args()
	if len(args) < 2 {
		printUsage()
		os.Exit(1)
	}
	if os.Geteuid() != 0 {
		fmt.Fprintf(os.Stderr, "ERROR: btrfs-snapshot-compress requires root (uses BTRFS ioctls). Run with sudo.\n")
		os.Exit(1)
	}

	mount := strings.TrimRight(args[0], "/")
	subvol := strings.TrimRight(args[1], "/")
	findFilter := args[2:]
	live := mount + "/" + subvol

	if _, err := os.Stat(live); err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: %s not found\n", live)
		os.Exit(1)
	}

	mountFd, err := syscall.Open(mount, syscall.O_RDONLY, 0)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: cannot open %s: %v\n", mount, err)
		os.Exit(1)
	}
	defer syscall.Close(mountFd)

	// Build extension blacklist
	extBlacklist := make(map[string]bool)
	if *skipExt {
		for _, e := range DEFAULT_INCOMPRESSIBLE_EXTS {
			extBlacklist[e] = true
		}
	}

	// Debug log
	var debugFile *os.File
	if *debugMs > 0 {
		debugFile, _ = os.OpenFile("debug.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if debugFile != nil {
			fmt.Fprintf(debugFile, "═══════════════════════════════════════════════════════════════════════════════\n")
			fmt.Fprintf(debugFile, "[%s] btrfs-snapshot-compress v%s started\n", time.Now().Format("15:04:05.000"), VERSION)
			fmt.Fprintf(debugFile, "═══════════════════════════════════════════════════════════════════════════════\n")
		}
	}
	if debugFile != nil {
		defer debugFile.Close()
	}

	fmt.Fprintf(os.Stderr, "btrfs-snapshot-compress v%s\n", VERSION)
	fmt.Fprintf(os.Stderr, "Mount:   %s\n", mount)
	fmt.Fprintf(os.Stderr, "Subvol:  %s\n", subvol)
	if len(findFilter) > 0 {
		fmt.Fprintf(os.Stderr, "Filter:  %s\n", strings.Join(findFilter, " "))
	}
	fmt.Fprintf(os.Stderr, "Workers: %d   Min-size: %s   Probe-ratio: %.2f×   Ext-blacklist: %v\n",
		*workers, fmtBytes(*minSize), *probeRatioMin, *skipExt)
	if *smartSpeedFlag {
		fmt.Fprintf(os.Stderr, "Smart-speed: ON   min-samples=%d   skip<%.0f%%   fast>%.0f%%   resample=1/%d\n",
			*smartMinSamples, *smartSkipThresh*100, *smartFastThresh*100, *smartResample)
	}
	ss := newSmartSpeed(*smartSpeedFlag, *smartMinSamples, *smartResample, *smartSkipThresh, *smartFastThresh)
	fmt.Fprintln(os.Stderr, "══════════════════════════════════════════════════════════════════════════════")
	fmt.Fprintln(os.Stderr, "  found     = files matched by walker")
	fmt.Fprintln(os.Stderr, "  buf       = files queued waiting")
	fmt.Fprintln(os.Stderr, "  checked   = processed/incompressible/notFound/changed")
	fmt.Fprintln(os.Stderr, "  skip      = ext-blacklist/below-min-size/nocow/already-compressed/smart-skip")
	fmt.Fprintln(os.Stderr, "  probed    = files where probe-compress was run (fast= bypassed by smart-speed)")
	fmt.Fprintln(os.Stderr, "  poor      = files skipped because probe ratio was below threshold")
	fmt.Fprintln(os.Stderr, "  cmp/refl  = extents recompressed / reflink siblings updated")
	fmt.Fprintln(os.Stderr, "  saved     = approx. on-disk bytes freed (uncompressed - compressed)")
	fmt.Fprintln(os.Stderr, "══════════════════════════════════════════════════════════════════════════════")

	fileQ := NewSpillQueue(QUEUE_LIMIT)
	defer fileQ.Cleanup()
	walkDone := make(chan struct{})
	var findDone atomic.Bool

	skipping := *startAt != ""
	if skipping {
		fmt.Fprintf(os.Stderr, "Resume: skipping until %s\n", *startAt)
	}

	go func() {
		findArgs := []string{live, "-type", "f"}
		if len(findFilter) > 0 {
			findArgs = append(findArgs, "(")
			for _, a := range findFilter {
				findArgs = append(findArgs, strings.Fields(a)...)
			}
			findArgs = append(findArgs, ")")
		}
		fmt.Fprintf(os.Stderr, "find %s\n", strings.Join(findArgs, " "))
		cmd := exec.Command("find", findArgs...)
		stdout, _ := cmd.StdoutPipe()
		cmd.Start()
		scanner := bufio.NewScanner(stdout)
		scanner.Buffer(make([]byte, 1024*1024), 1024*1024)
		for scanner.Scan() {
			path := scanner.Text()
			if skipping {
				rel := strings.TrimPrefix(path, live+"/")
				if rel < *startAt {
					continue
				}
				skipping = false
				fmt.Fprintf(os.Stderr, "Resume: starting at %s\n", strings.TrimPrefix(path, live+"/"))
			}
			fileQ.Push(path)
		}
		cmd.Wait()
		fileQ.Close()
		findDone.Store(true)
		close(walkDone)
	}()

	// Counters and status goroutine
	var cnt counters
	startTime := time.Now()
	statusDone := make(chan struct{})
	var sigStop atomic.Bool

	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		// First tick after 1s for early feedback
		time.Sleep(1 * time.Second)
		for {
			elapsed := int(time.Since(startTime).Seconds())
			totalFound := fileQ.Total()
			checked := cnt.checked.Load()

			etaStr := ""
			if findDone.Load() && totalFound > 0 && checked > 0 && checked < totalFound {
				rate := float64(checked) / float64(elapsed)
				if rate > 0 {
					etaStr = fmt.Sprintf(" ETA:%s", fmtTime(int(float64(totalFound-checked)/rate)))
				}
			} else if findDone.Load() && checked >= totalFound && cnt.pending.Load() == 0 {
				etaStr = " done"
			}
			savedStr := fmtBytes(cnt.bytesSaved.Load())
			fmt.Fprintf(os.Stderr,
				"  [%s] found=%d buf=%d checked=%d/%d/%d/%d skip=%d/%d/%d/%d/%d probed=%d(fast=%d) poor=%d cmp/refl=%d/%d saved=%s%s\n",
				fmtTime(elapsed), totalFound, fileQ.Buffered(),
				checked, cnt.incompressible.Load(), cnt.notFound.Load(), cnt.changed.Load(),
				cnt.skippedExt.Load(), cnt.skippedSize.Load(), cnt.skippedNocow.Load(), cnt.skippedAlready.Load(), cnt.skippedSmart.Load(),
				cnt.probed.Load(), cnt.fastpath.Load(), cnt.poorRatio.Load(),
				cnt.compressed.Load(), cnt.reflinks.Load(),
				savedStr, etaStr,
			)
			select {
			case <-statusDone:
				return
			case <-ticker.C:
			}
		}
	}()

	// Worker pool
	var wg sync.WaitGroup
	for i := 0; i < *workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				if sigStop.Load() {
					return
				}
				path, ok := fileQ.Pop()
				if !ok {
					if findDone.Load() {
						return
					}
					time.Sleep(50 * time.Millisecond)
					continue
				}
				processFile(path, mountFd, mount, extBlacklist, *probeRatioMin, *minSize, ss, &cnt)
			}
		}()
	}

	// Signal handling — graceful stop
	sigCh := make(chan os.Signal, 1)
	signalNotify(sigCh)
	go func() {
		<-sigCh
		fmt.Fprintln(os.Stderr, "\nReceived signal — stopping after current files…")
		sigStop.Store(true)
	}()

	<-walkDone
	wg.Wait()
	close(statusDone)

	elapsed := int(time.Since(startTime).Seconds())
	fmt.Fprintln(os.Stderr, "══════════════════════════════════════════════════════════════════════════════")
	fmt.Fprintf(os.Stderr, "Done in %s.\n", fmtTime(elapsed))
	fmt.Fprintf(os.Stderr, "  files checked:        %d\n", cnt.checked.Load())
	fmt.Fprintf(os.Stderr, "  ext-blacklisted:      %d\n", cnt.skippedExt.Load())
	fmt.Fprintf(os.Stderr, "  below min-size:       %d\n", cnt.skippedSize.Load())
	fmt.Fprintf(os.Stderr, "  NOCOW (chattr +C):    %d\n", cnt.skippedNocow.Load())
	fmt.Fprintf(os.Stderr, "  already compressed:   %d (extents)\n", cnt.skippedAlready.Load())
	fmt.Fprintf(os.Stderr, "  probed:               %d\n", cnt.probed.Load())
	fmt.Fprintf(os.Stderr, "  poor ratio (skipped): %d\n", cnt.poorRatio.Load())
	fmt.Fprintf(os.Stderr, "  extents compressed:   %d\n", cnt.compressed.Load())
	fmt.Fprintf(os.Stderr, "  reflinks updated:     %d\n", cnt.reflinks.Load())
	fmt.Fprintf(os.Stderr, "  approx. bytes saved:  %s\n", fmtBytes(cnt.bytesSaved.Load()))
	if *smartSpeedFlag {
		fmt.Fprintf(os.Stderr, "  smart-skipped files:  %d\n", cnt.skippedSmart.Load())
		fmt.Fprintf(os.Stderr, "  fastpath (no probe):  %d\n", cnt.fastpath.Load())
		// Print top 20 extensions by sample count
		stats := ss.snapshot()
		if len(stats) > 0 {
			type extLine struct {
				ext  string
				snap extStatSnap
			}
			lines := make([]extLine, 0, len(stats))
			for k, v := range stats {
				lines = append(lines, extLine{k, v})
			}
			// Sort by total desc
			for i := 0; i < len(lines); i++ {
				for j := i + 1; j < len(lines); j++ {
					if lines[j].snap.total > lines[i].snap.total {
						lines[i], lines[j] = lines[j], lines[i]
					}
				}
			}
			fmt.Fprintln(os.Stderr, "  top extensions (compressible-rate):")
			limit := len(lines)
			if limit > 20 {
				limit = 20
			}
			for i := 0; i < limit; i++ {
				l := lines[i]
				rate := 0.0
				if l.snap.total > 0 {
					rate = float64(l.snap.success) / float64(l.snap.total)
				}
				fmt.Fprintf(os.Stderr, "    .%-12s n=%-6d compressible=%.0f%%\n",
					l.ext, l.snap.total, rate*100)
			}
		}
	}

	// Suppress unused-import warnings for io
	_ = io.Discard
}
