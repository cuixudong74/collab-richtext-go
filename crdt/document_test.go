package crdt

import (
	"math/rand"
	"testing"
)

// 并发插入：两个端点在同一位置并发插入，无论应用顺序如何结果一致。
func TestConcurrentInsertConverges(t *testing.T) {
	a := NewDocument("A")
	b := NewDocument("B")

	opsA := []Op{a.LocalInsert(0, 'x'), a.LocalInsert(1, 'y')}
	opsB := []Op{b.LocalInsert(0, '1'), b.LocalInsert(1, '2')}

	for _, op := range opsB {
		a.Apply(op)
	}
	for _, op := range opsA {
		b.Apply(op)
	}
	if a.Text() != b.Text() {
		t.Fatalf("diverged: %q vs %q", a.Text(), b.Text())
	}

	// 反向顺序也应收敛到同一结果
	c := NewDocument("C")
	d := NewDocument("D")
	for i := len(opsB) - 1; i >= 0; i-- {
		c.Apply(opsB[i])
	}
	for i := len(opsA) - 1; i >= 0; i-- {
		c.Apply(opsA[i])
	}
	for _, op := range opsA {
		d.Apply(op)
	}
	for _, op := range opsB {
		d.Apply(op)
	}
	if c.Text() != a.Text() || d.Text() != a.Text() {
		t.Fatalf("order-dependent: %q %q %q", a.Text(), c.Text(), d.Text())
	}
}

// 重复投递不得产生二次影响。
func TestDuplicateApplyIsIdempotent(t *testing.T) {
	a := NewDocument("A")
	b := NewDocument("B")
	op := a.LocalInsert(0, 'x')
	for i := 0; i < 5; i++ {
		b.Apply(op)
	}
	if b.Text() != "x" {
		t.Fatalf("duplicate apply changed doc: %q", b.Text())
	}
}

// 乱序投递（含跨端依赖）最终收敛。
func TestOutOfOrderWithDeps(t *testing.T) {
	a := NewDocument("A")
	op1 := a.LocalInsert(0, 'a')
	op2 := a.LocalInsert(1, 'b') // 依赖 op1 的节点作为 parent
	op3 := a.LocalDelete(0)      // 依赖 op1

	b := NewDocument("B")
	for _, op := range []Op{op3, op2, op1} { // 完全逆序
		b.Apply(op)
	}
	if b.Text() != "b" {
		t.Fatalf("got %q want %q", b.Text(), "b")
	}
}

// 并发删除同一字符只生效一次。
func TestConcurrentDelete(t *testing.T) {
	a := NewDocument("A")
	a.LocalInsert(0, 'a')
	a.LocalInsert(1, 'b')
	b := NewDocument("B")
	for _, op := range a.logOps() {
		b.Apply(op)
	}
	da := a.LocalDelete(0)
	db := b.LocalDelete(0)
	a.Apply(db)
	b.Apply(da)
	if a.Text() != "b" || b.Text() != "b" {
		t.Fatalf("got %q %q", a.Text(), b.Text())
	}
}

// 属性与并发增删：结果确定；被删内容上的属性不牵连存活内容。
func TestAttrWithConcurrentEdits(t *testing.T) {
	a := NewDocument("A")
	for i, ch := range "hello" {
		a.LocalInsert(i, ch)
	}
	b := NewDocument("B")
	for _, op := range a.logOps() {
		b.Apply(op)
	}

	bold := a.LocalAttr(0, 5, "bold", "true") // A 给整个区间加粗
	del := b.LocalDelete(0)                   // B 并发删除 'h'
	ins := b.LocalInsert(2, 'X')              // B 并发在区间内插入

	a.Apply(del)
	a.Apply(ins)
	b.Apply(bold)

	if a.Text() != b.Text() {
		t.Fatalf("diverged: %q vs %q", a.Text(), b.Text())
	}
	// 被删除的 'h' 不在了；其余字符加粗
	if got := a.AttrsAt(0)["bold"]; got != "true" {
		t.Fatalf("pos0 bold=%q", got)
	}
	// 并发插入的 X 位于属性区间内，按规则获得属性（确定性行为）
	xPos := -1
	for i, r := range a.Text() {
		if r == 'X' {
			xPos = i
		}
	}
	if xPos < 0 || a.AttrsAt(xPos)["bold"] != "true" {
		t.Fatalf("X at %d attrs=%v", xPos, a.AttrsAt(xPos))
	}
}

// 属性 LWW：后写的覆盖先写的，与到达顺序无关。
func TestAttrLWW(t *testing.T) {
	a := NewDocument("A")
	b := NewDocument("B")
	a.LocalInsert(0, 'x')
	b.Apply(a.logOps()[0])

	o1 := a.LocalAttr(0, 1, "color", "red")
	o2 := b.LocalAttr(0, 1, "color", "blue")
	// o1.Lam 与 o2.Lam 均为 2？确保不同时：o2 在 b 上 lam=2，o1 在 a 上 lam=2。
	// 平手时按 Site 比较，结果确定。
	a.Apply(o2)
	b.Apply(o1)
	if a.AttrsAt(0)["color"] != b.AttrsAt(0)["color"] {
		t.Fatalf("attr diverged: %v vs %v", a.AttrsAt(0), b.AttrsAt(0))
	}
}

// 取消属性。
func TestAttrUnset(t *testing.T) {
	a := NewDocument("A")
	a.LocalInsert(0, 'x')
	a.LocalAttr(0, 1, "bold", "true")
	a.LocalAttr(0, 1, "bold", "")
	if _, ok := a.AttrsAt(0)["bold"]; ok {
		t.Fatal("bold should be unset")
	}
}

// 随机操作集合的确定性重放：固定种子下任意置换应用顺序都收敛。
func TestDeterministicReplay(t *testing.T) {
	const sites = 3
	const opsPerSite = 30
	docs := make([]*Document, sites)
	names := []SiteID{"A", "B", "C"}
	for i := range docs {
		docs[i] = NewDocument(names[i])
	}
	rnd := rand.New(rand.NewSource(42))
	var all []Op
	for step := 0; step < opsPerSite; step++ {
		for _, d := range docs {
			n := d.Len()
			switch rnd.Intn(4) {
			case 0:
				pos := 0
				if n > 0 {
					pos = rnd.Intn(n + 1)
				}
				all = append(all, d.LocalInsert(pos, rune('a'+rnd.Intn(26))))
			case 1:
				if n > 0 {
					all = append(all, d.LocalDelete(rnd.Intn(n)))
				}
			case 2:
				if n > 0 {
					s := rnd.Intn(n)
					e := s + 1 + rnd.Intn(n-s)
					all = append(all, d.LocalAttr(s, e, "bold", "true"))
				}
			case 3:
				if n > 0 {
					s := rnd.Intn(n)
					e := s + 1 + rnd.Intn(n-s)
					all = append(all, d.LocalAttr(s, e, "bold", ""))
				}
			}
		}
	}
	want := ""
	for trial := 0; trial < 20; trial++ {
		perm := rand.New(rand.NewSource(int64(trial))).Perm(len(all))
		d := NewDocument("R")
		for _, i := range perm {
			d.Apply(all[i])
		}
		got := d.Text()
		if trial == 0 {
			want = got
			continue
		}
		if got != want {
			t.Fatalf("trial %d diverged:\n%q\n%q", trial, got, want)
		}
	}
	if len(want) == 0 {
		t.Fatal("empty result, test ineffective")
	}
}

// 快照恢复后与完整历史端一致，且能继续收敛。
func TestSnapshotRestore(t *testing.T) {
	a := NewDocument("A")
	for i, ch := range "hello" {
		a.LocalInsert(i, ch)
	}
	a.LocalAttr(1, 3, "bold", "true")
	a.Compact()

	snap := a.Snapshot()
	data, err := EncodeSnapshot(snap)
	if err != nil {
		t.Fatal(err)
	}
	snap2, err := DecodeSnapshot(data)
	if err != nil {
		t.Fatal(err)
	}
	b := FromSnapshot("B", snap2)
	if b.Text() != "hello" {
		t.Fatalf("restored text %q", b.Text())
	}
	if b.AttrsAt(1)["bold"] != "true" || b.AttrsAt(0)["bold"] != "" {
		t.Fatalf("restored attrs wrong: %v %v", b.AttrsAt(0), b.AttrsAt(1))
	}
	// 恢复后继续收敛
	op := a.LocalInsert(5, '!')
	if !b.Apply(op) {
		t.Fatal("apply after restore failed")
	}
	if b.Text() != "hello!" {
		t.Fatalf("got %q", b.Text())
	}
}

// 压缩后 OpsSince 对过旧版本返回 ok=false。
func TestOpsSinceAfterCompact(t *testing.T) {
	a := NewDocument("A")
	op1 := a.LocalInsert(0, 'a')
	ops, ok := a.OpsSince(VersionVector{})
	if !ok || len(ops) != 1 {
		t.Fatalf("got %v %v", ops, ok)
	}
	a.Compact()
	if _, ok := a.OpsSince(VersionVector{}); ok {
		t.Fatal("should report compacted history unavailable")
	}
	op2 := a.LocalInsert(1, 'b')
	ops, ok = a.OpsSince(VersionVector{"A": op1.Seq})
	if !ok || len(ops) != 1 || ops[0].Seq != op2.Seq {
		t.Fatalf("incremental after compact failed: %v %v", ops, ok)
	}
}

// logOps 返回当前日志（测试辅助）。
func (d *Document) logOps() []Op {
	ops, _ := d.OpsSince(VersionVector{})
	return ops
}

// 快照恢复后，迟到的插入仍按快照中的属性区间继承属性。
func TestSnapshotAttrOpsAffectLateInserts(t *testing.T) {
	a := NewDocument("A")
	a.LocalInsert(0, 'a')
	a.LocalInsert(1, 'b')
	a.LocalAttr(0, 2, "bold", "true")
	a.Compact()

	b := FromSnapshot("B", a.Snapshot())
	// 迟到的新插入落在属性区间内
	ins := a.LocalInsert(1, 'X')
	if !b.Apply(ins) {
		t.Fatal("apply failed")
	}
	if b.AttrsAt(1)["bold"] != "true" {
		t.Fatalf("late insert should inherit bold, got %v", b.AttrsAt(1))
	}
	if a.AttrsAt(1)["bold"] != "true" {
		t.Fatalf("source side lost bold: %v", a.AttrsAt(1))
	}
}
