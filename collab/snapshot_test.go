package collab

import (
	"testing"
)

// TestSnapshotRestore 验证快照保存与恢复。
func TestSnapshotRestore(t *testing.T) {
	d := NewDocument(1)
	d.Insert(0, "hello")
	d.SetAttr(0, 5, "bold", true)

	snap := d.Snapshot()

	d2 := NewDocument(2)
	d2.Restore(snap)

	if d2.Text() != "hello" {
		t.Fatalf("restored text: %q", d2.Text())
	}
	spans := d2.RichText()
	if len(spans) != 1 || !spans[0].Attrs["bold"] {
		t.Fatalf("restored attrs: %+v", spans)
	}
}

// TestCompactConvergence 验证历史压缩后仍与完整历史端收敛。
func TestCompactConvergence(t *testing.T) {
	// 完整历史端（client 1）
	full := NewDocument(1)
	aOps := full.Insert(0, "abc")
	delOps := full.Delete(1, 2)
	insOps := full.Insert(1, "x")

	// 压缩端（client 2）从完整端接收操作
	compacted := NewDocument(2)
	for _, op := range aOps {
		compacted.Apply(op)
	}
	for _, op := range delOps {
		compacted.Apply(op)
	}
	for _, op := range insOps {
		compacted.Apply(op)
	}

	// 压缩
	removed := compacted.Compact()
	t.Logf("removed %d tombstones", removed)

	// 两端正文一致
	if full.Text() != compacted.Text() {
		t.Fatalf("diverge after compact: %q vs %q", full.Text(), compacted.Text())
	}

	// 压缩端仍能接收新操作并收敛
	newOps := full.Insert(2, "y")
	for _, op := range newOps {
		compacted.Apply(op)
	}
	if full.Text() != compacted.Text() {
		t.Fatalf("diverge after new op: %q vs %q", full.Text(), compacted.Text())
	}
}

// TestNewMemberFromSnapshot 验证新参与者从快照+增量正确对齐。
func TestNewMemberFromSnapshot(t *testing.T) {
	// 老参与者
	old := NewDocument(1)
	old.Insert(0, "abc")
	old.SetAttr(0, 3, "bold", true)

	// 新参与者从快照恢复
	snap := old.Snapshot()
	new := NewDocument(2)
	new.Restore(snap)

	// 老参与者产生新操作
	newOps := old.Insert(3, "def")
	for _, op := range newOps {
		new.Apply(op)
	}

	if old.Text() != new.Text() {
		t.Fatalf("diverge: %q vs %q", old.Text(), new.Text())
	}
	spans := new.RichText()
	if len(spans) != 2 {
		t.Fatalf("expected 2 spans, got %d", len(spans))
	}
}

// TestSnapshotVV 验证快照包含版本向量。
func TestSnapshotVV(t *testing.T) {
	d := NewDocument(1)
	d.Insert(0, "ab")
	snap := d.Snapshot()
	if snap.VV[1] != 2 {
		t.Fatalf("expected vv[1]=2, got %d", snap.VV[1])
	}
}
