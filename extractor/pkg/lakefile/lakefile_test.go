package lakefile

import (
	"os"
	"path/filepath"
	"testing"
)

func TestWriteCreatesNewVersionAndMovesFrontier(t *testing.T) {
	frontier := filepath.Join(t.TempDir(), "year=2026", "month=10", "day=20", "20261020_0100.json")

	first, v1, err := Write(frontier, []byte("first\n"))
	if err != nil {
		t.Fatal(err)
	}
	second, v2, err := Write(frontier, []byte("second\n"))
	if err != nil {
		t.Fatal(err)
	}

	if v1 != 1 || v2 != 2 {
		t.Fatalf("versions = %d, %d; want 1, 2", v1, v2)
	}
	if filepath.Base(first) != "20261020_0100.v1.json" || filepath.Base(second) != "20261020_0100.v2.json" {
		t.Fatalf("paths = %s, %s", first, second)
	}

	target, err := os.Readlink(frontier)
	if err != nil {
		t.Fatal(err)
	}
	if target != "20261020_0100.v2.json" {
		t.Fatalf("frontier -> %q; want relative link to v2", target)
	}

	assertContent(t, frontier, "second\n")
	assertContent(t, first, "first\n")

}

func assertContent(t *testing.T, path, want string) {
	t.Helper()
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != want {
		t.Fatalf("%s = %q; want %q", path, got, want)
	}
}
