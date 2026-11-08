package worker

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// cacheAttestation binds a verified content digest to the exact inode that was
// hashed. A cache hit is revalidated with stat metadata only while dev, inode,
// size, mtime and ctime all match; any replacement or write invalidates it and
// forces the canonical full-file SHA-256 check.
type cacheAttestation struct {
	SchemaVersion int    `json:"schema_version"`
	SHA256        string `json:"sha256"`
	SizeBytes     int64  `json:"size_bytes"`
	Device        uint64 `json:"device"`
	Inode         uint64 `json:"inode"`
	MtimeNS       int64  `json:"mtime_ns"`
	CtimeNS       int64  `json:"ctime_ns"`
	// SampleSHA256 fingerprints up to cacheSampleBytes of the blob. Timestamp
	// granularity is not portable (some filesystems only expose whole seconds),
	// so the stat identity alone cannot detect a same-tick rewrite. The sample
	// keeps the revalidation read bounded while still catching replacement or
	// middle-of-file corruption for small fragments.
	SampleSHA256 string `json:"sample_sha256"`
	SampleBytes  int64  `json:"sample_bytes"`
}

const (
	cacheAttestationSchema = 1
	cacheSampleBytes       = 256 << 10
)

var (
	cacheAttestationMemory sync.Map // map[string]cacheAttestation
	// sha256FileForCache is the cache-probe hash seam. Promotion still uses
	// sha256File directly: a newly written blob is always hashed before it
	// becomes visible.
	sha256FileForCache = sha256File
)

func cacheAttestationPath(cacheDir, sha256Hex string) string {
	return filepath.Join(cacheDir, "attest", sha256Hex+".json")
}

func cacheAttestationKey(path string, info os.FileInfo) (string, cacheAttestation, bool) {
	dev, ino, ctimeNS, ok := statIdentity(info)
	if !ok {
		return "", cacheAttestation{}, false
	}
	entry := cacheAttestation{
		SchemaVersion: cacheAttestationSchema,
		SizeBytes:     info.Size(),
		Device:        dev,
		Inode:         ino,
		MtimeNS:       info.ModTime().UnixNano(),
		CtimeNS:       ctimeNS,
	}
	key := fmt.Sprintf("%s|%d|%d|%d|%d|%d", path, dev, ino, info.Size(), entry.MtimeNS, ctimeNS)
	return key, entry, true
}

// attestCacheBlob reports whether path is still the exact inode that was
// SHA-256 verified for expectedSHA256. The in-process entry avoids repeated
// sidecar reads; the durable sidecar survives worker restarts.
func attestCacheBlob(cacheDir, path, expectedSHA256 string, info os.FileInfo) bool {
	key, current, ok := cacheAttestationKey(path, info)
	if !ok {
		return false
	}
	sample, sampleBytes, err := cacheSampleDigest(path, info.Size())
	if err != nil {
		return false
	}
	current.SHA256, current.SampleSHA256, current.SampleBytes = expectedSHA256, sample, sampleBytes
	if cached, found := cacheAttestationMemory.Load(key); found {
		entry, _ := cached.(cacheAttestation)
		return entry.SchemaVersion == cacheAttestationSchema && entry.SHA256 == expectedSHA256 &&
			entry.SampleSHA256 == sample && entry.SampleBytes == sampleBytes
	}
	raw, err := os.ReadFile(cacheAttestationPath(cacheDir, expectedSHA256))
	if err != nil {
		return false
	}
	var entry cacheAttestation
	if err := json.Unmarshal(raw, &entry); err != nil {
		return false
	}
	if entry.SchemaVersion != cacheAttestationSchema || entry.SHA256 != expectedSHA256 ||
		entry.SizeBytes != current.SizeBytes || entry.Device != current.Device ||
		entry.Inode != current.Inode || entry.MtimeNS != current.MtimeNS || entry.CtimeNS != current.CtimeNS ||
		entry.SampleSHA256 != sample || entry.SampleBytes != sampleBytes {
		return false
	}
	cacheAttestationMemory.Store(key, entry)
	return true
}

func writeCacheAttestation(cacheDir, path, sha256Hex string, info os.FileInfo) error {
	key, entry, ok := cacheAttestationKey(path, info)
	if !ok {
		return nil
	}
	entry.SHA256 = sha256Hex
	sample, sampleBytes, err := cacheSampleDigest(path, info.Size())
	if err != nil {
		return err
	}
	entry.SampleSHA256, entry.SampleBytes = sample, sampleBytes
	encoded, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	dir := filepath.Dir(cacheAttestationPath(cacheDir, sha256Hex))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".attest-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	if _, err := tmp.Write(encoded); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, cacheAttestationPath(cacheDir, sha256Hex)); err != nil {
		return err
	}
	cacheAttestationMemory.Store(key, entry)
	return nil
}

func removeCacheAttestation(cacheDir, sha256Hex string) {
	if cacheDir == "" || sha256Hex == "" {
		return
	}
	_ = os.Remove(cacheAttestationPath(cacheDir, sha256Hex))
}

// cacheSampleDigest hashes up to cacheSampleBytes of the blob: the whole file
// when it is small, otherwise the head and tail halves plus the total size.
// It bounds the revalidation read to 256 KiB while still detecting a replaced
// file whose stat identity was preserved by coarse filesystem timestamps.
func cacheSampleDigest(path string, size int64) (string, int64, error) {
	if size <= 0 {
		return "", 0, fmt.Errorf("cache attestation: empty blob")
	}
	file, err := os.Open(path)
	if err != nil {
		return "", 0, err
	}
	defer file.Close()
	hasher := sha256.New()
	fmt.Fprintf(hasher, "%d:", size)
	if size <= cacheSampleBytes {
		if _, err := io.Copy(hasher, file); err != nil {
			return "", 0, err
		}
		return hex.EncodeToString(hasher.Sum(nil)), size, nil
	}
	half := int64(cacheSampleBytes / 2)
	head := make([]byte, half)
	if _, err := io.ReadFull(file, head); err != nil {
		return "", 0, err
	}
	if _, err := hasher.Write(head); err != nil {
		return "", 0, err
	}
	if _, err := file.Seek(size-half, io.SeekStart); err != nil {
		return "", 0, err
	}
	tail := make([]byte, half)
	if _, err := io.ReadFull(file, tail); err != nil {
		return "", 0, err
	}
	if _, err := hasher.Write(tail); err != nil {
		return "", 0, err
	}
	return hex.EncodeToString(hasher.Sum(nil)), cacheSampleBytes, nil
}

func sameCacheIdentity(a, b os.FileInfo) bool {
	if a == nil || b == nil || a.Size() != b.Size() || !a.ModTime().Equal(b.ModTime()) {
		return false
	}
	aDev, aIno, aCtime, aOK := statIdentity(a)
	bDev, bIno, bCtime, bOK := statIdentity(b)
	return aOK && bOK && aDev == bDev && aIno == bIno && aCtime == bCtime
}
