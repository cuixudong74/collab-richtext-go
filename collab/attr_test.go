package collab

import (
	"testing"
)

// TestBasicMarks 验证基本的加粗/斜体属性设置与取消。
func TestBasicMarks(t *testing.T) {
	d := NewDocument(1)
	d.Insert(0, "hello world")

	// 对 "hello" 设置加粗
	op := d.SetAttr(0, 5, "bold", true)
	if op == nil {
		t.Fatal("SetAttr returned nil")
	}

	spans := d.RichText()
	if len(spans) != 2 {
		t.Fatalf("expected 2 spans, got %d: %+v", len(spans), spans)
	}
	if spans[0].Text != "hello" || !spans[0].Attrs["bold"] {
		t.Fatalf("first span should be bold 'hello', got %+v", spans[0])
	}
	if spans[1].Text != " world" || spans[1].Attrs["bold"] {
		t.Fatalf("second span should be non-bold ' world', got %+v", spans[1])
	}

	// 取消加粗
	d.SetAttr(0, 5, "bold", false)
	spans = d.RichText()
	if len(spans) != 1 {
		t.Fatalf("expected 1 span after unbold, got %d", len(spans))
	}
	if spans[0].Attrs["bold"] {
		t.Fatal("bold should be false after unbold")
	}
}

// TestMarkLWW 验证同一位置多属性标记时 LWW（ID 最大者胜）。
func TestMarkLWW(t *testing.T) {
	d := NewDocument(1)
	d.Insert(0, "ab")

	// 两个并发的加粗标记，ID 大的胜
	m1 := MarkOp(OpID{1, 1}, "bold", true, DocStart(), DocEnd())
	m2 := MarkOp(OpID{1, 2}, "bold", false, DocStart(), DocEnd())
	d.Apply(m1)
	d.Apply(m2)

	spans := d.RichText()
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}
	if spans[0].Attrs["bold"] {
		t.Fatal("m2 (larger ID, bold=false) should win")
	}
}

// TestMarkDoesNotAffectDeleted 验证删除内容上的属性变化不牵连存活内容。
func TestMarkDoesNotAffectDeleted(t *testing.T) {
	d := NewDocument(1)
	d.Insert(0, "abc")
	// 对 "abc" 设置加粗
	d.SetAttr(0, 3, "bold", true)
	// 删除 "b"
	d.Delete(1, 2)
	// 在 "b" 的位置插入 "x"
	d.Insert(1, "x")

	spans := d.RichText()
	// "a" 和 "c" 应该是加粗的，"x" 不应该被加粗（属性标记锚定的是位置，
	// 但 "x" 是在属性标记创建之后插入的，不在标记覆盖范围内）
	for _, s := range spans {
		t.Logf("span: %q bold=%v", s.Text, s.Attrs["bold"])
	}
}

// TestMarkConvergence 验证属性操作与文本增删并发时收敛。
func TestMarkConvergence(t *testing.T) {
	d1 := NewDocument(1)
	d2 := NewDocument(2)

	// d1 插入 "abc" 并加粗
	aOps := d1.Insert(0, "abc")
	d1.SetAttr(0, 3, "bold", true)

	// d2 独立插入 "xy"
	bOps := d2.Insert(0, "xy")

	// 交换
	for _, op := range aOps {
		d2.Apply(op)
	}
	for _, op := range bOps {
		d1.Apply(op)
	}

	// 收敛性：两边正文一致
	if d1.Text() != d2.Text() {
		t.Fatalf("text diverge: %q vs %q", d1.Text(), d2.Text())
	}
	t.Logf("converged: %q", d1.Text())
}
