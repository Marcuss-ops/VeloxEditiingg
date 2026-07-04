// velox-bundler builds the worker bundle zip consumed by ansible playbooks
// and the /api/worker/bundle endpoint. It packages VERSION.txt + RemoteCodex/
// + shared/ from the repo root, excluding .git, test files, and testdata.
//
// Usage:
//
//	velox-bundler --source <repo-root> --output <output-dir>
//
// Produces: <output-dir>/worker_code_linux_x86_64.zip
package main

import (
	"archive/zip"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"strings"
)

func main() {
	sourceDir := flag.String("source", "", "Repo root directory (required)")
	outputDir := flag.String("output", "", "Output directory for the bundle zip (required)")
	flag.Parse()

	if *sourceDir == "" {
		log.Fatal("--source is required")
	}
	if *outputDir == "" {
		log.Fatal("--output is required")
	}

	src, err := filepath.Abs(*sourceDir)
	if err != nil {
		log.Fatalf("resolve source dir: %v", err)
	}
	out, err := filepath.Abs(*outputDir)
	if err != nil {
		log.Fatalf("resolve output dir: %v", err)
	}

	if err := os.MkdirAll(out, 0755); err != nil {
		log.Fatalf("create output dir: %v", err)
	}

	zipPath := filepath.Join(out, "worker_code_linux_x86_64.zip")
	f, err := os.Create(zipPath)
	if err != nil {
		log.Fatalf("create zip: %v", err)
	}
	defer f.Close()

	zw := zip.NewWriter(f)
	defer zw.Close()

	// 1. VERSION.txt
	addFileToZip(zw, src, filepath.Join(src, "VERSION.txt"))

	// 2. RemoteCodex/
	walkDirToZip(zw, src, filepath.Join(src, "RemoteCodex"))

	// 3. shared/
	walkDirToZip(zw, src, filepath.Join(src, "shared"))

	if err := zw.Close(); err != nil {
		log.Fatalf("close zip writer: %v", err)
	}
	if err := f.Close(); err != nil {
		log.Fatalf("close zip file: %v", err)
	}

	stat, _ := os.Stat(zipPath)
	sizeMB := float64(0)
	if stat != nil {
		sizeMB = float64(stat.Size()) / (1024 * 1024)
	}
	fmt.Printf("Bundle written: %s (%.1f MB)\n", zipPath, sizeMB)
}

func addFileToZip(zw *zip.Writer, baseDir, path string) {
	rel, err := filepath.Rel(baseDir, path)
	if err != nil {
		log.Printf("skip %s: %v", path, err)
		return
	}

	info, err := os.Stat(path)
	if err != nil {
		log.Printf("skip %s: %v", path, err)
		return
	}

	hdr, err := zip.FileInfoHeader(info)
	if err != nil {
		log.Printf("skip %s: %v", path, err)
		return
	}
	hdr.Name = filepath.ToSlash(rel)
	hdr.Method = zip.Deflate

	w, err := zw.CreateHeader(hdr)
	if err != nil {
		log.Printf("skip %s: %v", path, err)
		return
	}

	f, err := os.Open(path)
	if err != nil {
		log.Printf("skip %s: %v", path, err)
		return
	}
	defer f.Close()

	if _, err := io.Copy(w, f); err != nil {
		log.Printf("copy %s: %v", path, err)
	}
}

func walkDirToZip(zw *zip.Writer, baseDir, dir string) {
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}

		rel, err := filepath.Rel(baseDir, path)
		if err != nil {
			return nil
		}
		relSlash := filepath.ToSlash(rel)

		// Skip .git, build, testdata, and hidden directories.
		if d.IsDir() {
			name := filepath.Base(path)
			if name == ".git" || name == "build" || name == "testdata" || strings.HasPrefix(name, ".") && name != "." && name != ".." {
				return filepath.SkipDir
			}
			return nil
		}

		// Skip test files.
		if strings.HasSuffix(path, "_test.go") {
			return nil
		}

		info, err := d.Info()
		if err != nil {
			return nil
		}

		hdr, err := zip.FileInfoHeader(info)
		if err != nil {
			return nil
		}
		hdr.Name = relSlash
		hdr.Method = zip.Deflate

		w, err := zw.CreateHeader(hdr)
		if err != nil {
			return nil
		}

		f, err := os.Open(path)
		if err != nil {
			return nil
		}

		if _, err := io.Copy(w, f); err != nil {
			f.Close()
			log.Printf("copy %s: %v", relSlash, err)
			return nil
		}
		f.Close()
		return nil
	})

	// filepath.SkipDir is not an error; prevent WalkDir from surfacing it.
	if err != nil && err != filepath.SkipDir {
		log.Printf("walk %s: %v", dir, err)
	}
}
