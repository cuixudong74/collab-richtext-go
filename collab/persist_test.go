package collab

import (
	"os"
	"testing"
)

// TestSnapshotPersistence 验证快照保存到文件后可恢复。
func TestSnapshotPersistence(t *testing.T) {
	d := NewDocument(1)
	d.Insert(0, "hello")
	d.SetAttr(0, 5, "bold", true)

	snap := d.Snapshot()
	path := "/tmp/collab_snapshot_test.json"
	defer os.Remove(path)

	if err := SaveSnapshot(snap, path); err != nil {
		t.Fatal(err)
	}

	loaded, err := LoadSnapshot(path)
	if err != nil {
		t.Fatal(err)
	}

	d2 := NewDocument(2)
	d2.Restore(loaded)

	if d2.Text() != "hello" {
		t.Fatalf("restored text: %q", d2.Text())
	}
	spans := d2.RichText()
	if len(spans) != 1 || !spans[0].Attrs["bold"] {
		t.Fatalf("restored attrs: %+v", spans)
	}
}
