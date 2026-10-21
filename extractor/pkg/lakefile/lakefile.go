package lakefile

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// Write adds a new version (name.vN.ext) and repoints the name.ext symlink to it.
func Write(frontier string, content []byte) (string, int, error) {
	dir := filepath.Dir(frontier)
	ext := filepath.Ext(frontier)
	stem := strings.TrimSuffix(filepath.Base(frontier), ext)

	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", 0, err
	}

	version, err := nextVersion(dir, stem, ext)
	if err != nil {
		return "", 0, err
	}
	name := fmt.Sprintf("%s.v%d%s", stem, version, ext)
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, content, 0o644); err != nil {
		return "", 0, err
	}

	// Relative target so the link resolves both inside the container and on the host.
	tmp := frontier + ".tmp"
	if err := os.Symlink(name, tmp); err != nil {
		return "", 0, err
	}
	if err := os.Rename(tmp, frontier); err != nil {
		return "", 0, err
	}
	return path, version, nil
}

func nextVersion(dir, stem, ext string) (int, error) {
	matches, err := filepath.Glob(filepath.Join(dir, stem+".v*"+ext))
	if err != nil {
		return 0, err
	}
	latest := 0
	for _, m := range matches {
		n := strings.TrimSuffix(strings.TrimPrefix(filepath.Base(m), stem+".v"), ext)
		if v, err := strconv.Atoi(n); err == nil && v > latest {
			latest = v
		}
	}
	return latest + 1, nil
}
