package main

import (
	"archive/zip"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"
)

var Version = "dev"

type installRecord struct {
	InstalledAt string `json:"installed_at"`
	Version     string `json:"version"`
	MasterURL   string `json:"master_url,omitempty"`
	WorkerName  string `json:"worker_name,omitempty"`
	TargetDir   string `json:"target_dir,omitempty"`
	PackageSrc  string `json:"package_source,omitempty"`
}

func main() {
	var masterURL string
	var workerName string
	var targetDir string
	var packageSource string

	flag.StringVar(&masterURL, "master-url", "", "master URL used for this install")
	flag.StringVar(&workerName, "worker-name", "", "worker name for metadata")
	flag.StringVar(&targetDir, "target-dir", "/opt/VeloxEditing", "installation target directory")
	flag.StringVar(&packageSource, "package-source", "", "zip package to unpack")
	flag.Parse()

	if strings.TrimSpace(masterURL) == "" {
		log.Fatal("--master-url is required")
	}
	if strings.TrimSpace(packageSource) == "" {
		log.Fatal("--package-source is required")
	}

	absTarget, err := filepath.Abs(targetDir)
	if err != nil {
		log.Fatalf("invalid target dir: %v", err)
	}
	absPackage, err := filepath.Abs(packageSource)
	if err != nil {
		log.Fatalf("invalid package source: %v", err)
	}

	currentDir := filepath.Join(absTarget, "current")
	bootstrapDir := filepath.Join(currentDir, "bootstrap")
	if err := os.MkdirAll(bootstrapDir, 0755); err != nil {
		log.Fatalf("failed to create target dirs: %v", err)
	}

	if err := copyFileSelf(filepath.Join(currentDir, "velox-installer")); err != nil {
		log.Printf("warning: unable to persist installer copy: %v", err)
	}

	if err := unpackZip(absPackage, filepath.Join(bootstrapDir, "package")); err != nil {
		log.Fatalf("failed to unpack package: %v", err)
	}

	if err := flattenBundle(filepath.Join(bootstrapDir, "package"), currentDir); err != nil {
		log.Fatalf("failed to flatten bundle layout: %v", err)
	}

	if err := ensureReleaseSnapshot(absTarget, currentDir); err != nil {
		log.Fatalf("failed to prepare release snapshot: %v", err)
	}

	rec := installRecord{
		InstalledAt: time.Now().UTC().Format(time.RFC3339),
		Version:     Version,
		MasterURL:   masterURL,
		WorkerName:  workerName,
		TargetDir:   absTarget,
		PackageSrc:  absPackage,
	}
	if err := writeJSON(filepath.Join(currentDir, "install_record.json"), rec); err != nil {
		log.Printf("warning: unable to write install record: %v", err)
	}

	fmt.Printf("Velox installer complete\n")
	fmt.Printf("Target: %s\n", absTarget)
	fmt.Printf("Package: %s\n", absPackage)
	fmt.Printf("Worker: %s\n", workerName)
}

func copyFileSelf(dst string) error {
	src, err := os.Executable()
	if err != nil {
		return err
	}
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	if err := os.MkdirAll(filepath.Dir(dst), 0755); err != nil {
		return err
	}
	tmp := dst + ".tmp"
	out, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		os.Remove(tmp)
		return err
	}
	if err := out.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, dst)
}

func unpackZip(srcZip, dstDir string) error {
	zr, err := zip.OpenReader(srcZip)
	if err != nil {
		return err
	}
	defer zr.Close()

	if err := os.MkdirAll(dstDir, 0755); err != nil {
		return err
	}

	for _, f := range zr.File {
		targetPath := filepath.Join(dstDir, f.Name)
		if !strings.HasPrefix(filepath.Clean(targetPath), filepath.Clean(dstDir)+string(os.PathSeparator)) && filepath.Clean(targetPath) != filepath.Clean(dstDir) {
			return fmt.Errorf("zip entry escapes destination: %s", f.Name)
		}
		if f.FileInfo().IsDir() {
			if err := os.MkdirAll(targetPath, f.Mode()); err != nil {
				return err
			}
			continue
		}
		if err := os.MkdirAll(filepath.Dir(targetPath), 0755); err != nil {
			return err
		}
		rc, err := f.Open()
		if err != nil {
			return err
		}
		out, err := os.Create(targetPath)
		if err != nil {
			rc.Close()
			return err
		}
		if _, err := io.Copy(out, rc); err != nil {
			rc.Close()
			out.Close()
			return err
		}
		rc.Close()
		if err := out.Close(); err != nil {
			return err
		}
		if err := os.Chmod(targetPath, f.Mode()); err != nil {
			_ = err
		}
	}
	return nil
}

func flattenBundle(packageRoot, currentDir string) error {
	refactoredRoot := filepath.Join(packageRoot, "refactored")
	if _, err := os.Stat(refactoredRoot); err != nil {
		// Already flat or no bundle root; nothing to do.
		return nil
	}

	entries, err := os.ReadDir(refactoredRoot)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		src := filepath.Join(refactoredRoot, entry.Name())
		dst := filepath.Join(currentDir, entry.Name())
		if err := os.RemoveAll(dst); err != nil {
			return err
		}
		if err := os.Rename(src, dst); err != nil {
			if err := copyPath(src, dst); err != nil {
				return err
			}
			_ = os.RemoveAll(src)
		}
	}
	return nil
}

func copyPath(src, dst string) error {
	info, err := os.Stat(src)
	if err != nil {
		return err
	}
	if info.IsDir() {
		return filepath.Walk(src, func(path string, fi os.FileInfo, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			rel, err := filepath.Rel(src, path)
			if err != nil {
				return err
			}
			target := filepath.Join(dst, rel)
			if fi.IsDir() {
				return os.MkdirAll(target, fi.Mode())
			}
			data, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			return os.WriteFile(target, data, fi.Mode())
		})
	}

	data, err := os.ReadFile(src)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(dst), 0755); err != nil {
		return err
	}
	return os.WriteFile(dst, data, info.Mode())
}

func ensureReleaseSnapshot(targetDir, currentDir string) error {
	releasesDir := filepath.Join(targetDir, "releases")
	if err := os.MkdirAll(releasesDir, 0755); err != nil {
		return err
	}

	releaseDir := filepath.Join(releasesDir, time.Now().UTC().Format("20060102-150405"))
	if err := os.MkdirAll(releaseDir, 0755); err != nil {
		return err
	}

	linkPath := filepath.Join(releaseDir, "refactored")
	if err := os.RemoveAll(linkPath); err != nil {
		return err
	}
	if err := os.Symlink(currentDir, linkPath); err != nil {
		return err
	}

	return nil
}

func writeJSON(path string, v interface{}) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0644)
}
