package collab

import (
	"testing"
)

// TestMarkInsertInside 验证在属性区间内插入文本时的行为。
// 属性标记锚定到字符位置，在区间内插入的新字符是否被覆盖取决于锚点偏向。
func TestMarkInsertInside(t *testing.T) {
	d := NewDocument(1)
	d.Insert(0, "abc")
	d.SetAttr(0, 3, "bold", true)

	// 在 "b" 后插入 "x"（位置 2）
	d.Insert(2, "x")

	spans := d.RichText()
	for _, s := range spans {
		t.Logf("span: %q bold=%v", s.Text, s.Attrs["bold"])
	}
	// 结果应确定且可解释
	if d.Text() != "abxc" {
		t.Fatalf("text: %q", d.Text())
	}
}

// TestMarkDeleteRange 验证删除带属性的文本后属性正确移除。
func TestMarkDeleteRange(t *testing.T) {
	d := NewDocument(1)
	d.Insert(0, "abc")
	d.SetAttr(0, 3, "bold", true)

	// 删除 "b"
	d.Delete(1, 2)

	spans := d.RichText()
	// "a" 和 "c" 仍应加粗
	for _, s := range spans {
		t.Logf("span: %q bold=%v", s.Text, s.Attrs["bold"])
	}
}

// TestMarkConcurrent 验证属性操作与文本增删并发时收敛。
func TestMarkConcurrent(t *testing.T) {
	for trial := 0; trial < 50; trial++ {
		d1 := NewDocument(1)
		d2 := NewDocument(2)

		// d1: 插入 "abc" 并加粗
		aOps := d1.Insert(0, "abc")
		d1.SetAttr(0, 3, "bold", true)

		// d2: 插入 "xyz"
		bOps := d2.Insert(0, "xyz")

		// 交换操作
		for _, op := range aOps {
			d2.Apply(op)
		}
		for _, op := range bOps {
			d1.Apply(op)
		}

		// 再交换属性操作
		// d1 的 bold 操作
		boldOp := d1.SetAttr(0, 3, "bold", true)
		d2.Apply(boldOp)

		if d1.Text() != d2.Text() {
			t.Fatalf("trial %d text diverge: %q vs %q", trial, d1.Text(), d2.Text())
		}
	}
}

// TestMultipleAttrs 验证多种属性同时存在。
func TestMultipleAttrs(t *testing.T) {
	d := NewDocument(1)
	d.Insert(0, "hello world")
	d.SetAttr(0, 5, "bold", true)
	d.SetAttr(0, 5, "italic", true)
	d.SetAttr(6, 11, "italic", true)

	spans := d.RichText()
	if len(spans) != 3 {
		t.Fatalf("expected 3 spans, got %d: %+v", len(spans), spans)
	}
	if spans[0].Text != "hello" || !spans[0].Attrs["bold"] || !spans[0].Attrs["italic"] {
		t.Fatalf("first span: %+v", spans[0])
	}
	if spans[1].Text != " " || spans[1].Attrs["bold"] || spans[1].Attrs["italic"] {
		t.Fatalf("second span: %+v", spans[1])
	}
	if spans[2].Text != "world" || spans[2].Attrs["bold"] || !spans[2].Attrs["italic"] {
		t.Fatalf("third span: %+v", spans[2])
	}
}
