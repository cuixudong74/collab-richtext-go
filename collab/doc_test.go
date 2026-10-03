package collab

import (
	"math/rand"
	"testing"
)

// TestRGAConverges 验证同一组操作以不同顺序应用后正文一致。
func TestRGAConverges(t *testing.T) {
	// 构造一组操作：两个参与者并发插入
	d1 := NewDocument(1)
	d2 := NewDocument(2)

	// 参与者1: "abc"
	var ops []*Op
	ops = append(ops, d1.Insert(0, "abc")...)
	// 参与者2 在 d1 的基础上... 不行，需要独立构造
	_ = d2
	_ = ops

	// 更直接：手工构造操作序列
	docA := NewDocument(1)
	docB := NewDocument(2)

	// A 插入 "ab"
	aOps := docA.Insert(0, "ab")
	// B 插入 "xy"（独立起点）
	bOps := docB.Insert(0, "xy")

	// 交换操作
	for _, op := range aOps {
		if err := docB.Apply(op); err != nil {
			t.Fatal(err)
		}
	}
	for _, op := range bOps {
		if err := docA.Apply(op); err != nil {
			t.Fatal(err)
		}
	}

	if docA.Text() != docB.Text() {
		t.Fatalf("diverge: A=%q B=%q", docA.Text(), docB.Text())
	}
	t.Logf("converged text: %q", docA.Text())
}

// TestRGAConvergesRandom 随机操作集合、随机顺序下验证收敛。
func TestRGAConvergesRandom(t *testing.T) {
	for trial := 0; trial < 200; trial++ {
		seed := int64(trial)
		rng := rand.New(rand.NewSource(seed))

		// 生成 3 个参与者的操作
		docs := []*Document{NewDocument(1), NewDocument(2), NewDocument(3)}
		var allOps []*Op
		for i := 0; i < 30; i++ {
			di := rng.Intn(3)
			doc := docs[di]
			pos := rng.Intn(len([]rune(doc.Text())) + 1)
			ch := rune('a' + rng.Intn(26))
			ops := doc.Insert(pos, string(ch))
			allOps = append(allOps, ops...)
		}

		// 以两种独立随机顺序应用到新文档
		order1 := make([]*Op, len(allOps))
		order2 := make([]*Op, len(allOps))
		copy(order1, allOps)
		copy(order2, allOps)
		rng.Shuffle(len(order1), func(i, j int) { order1[i], order1[j] = order1[j], order1[i] })
		rng.Shuffle(len(order2), func(i, j int) { order2[i], order2[j] = order2[j], order2[i] })

		x := NewDocument(1)
		y := NewDocument(2)
		for _, op := range order1 {
			if err := x.Apply(op); err != nil {
				t.Fatal(err)
			}
		}
		for _, op := range order2 {
			if err := y.Apply(op); err != nil {
				t.Fatal(err)
			}
		}
		if x.Text() != y.Text() {
			t.Fatalf("trial %d diverge: %q vs %q", trial, x.Text(), y.Text())
		}
	}
}

// TestDeleteIdempotent 重复投递删除不产生二次影响。
func TestDeleteIdempotent(t *testing.T) {
	d := NewDocument(1)
	ops := d.Insert(0, "abc")
	if len(ops) != 3 {
		t.Fatal("expected 3 ops")
	}
	del := d.Delete(1, 2)
	if len(del) != 1 {
		t.Fatal("expected 1 delete")
	}
	// 重复投递删除
	if err := d.Apply(del[0]); err != nil {
		t.Fatal(err)
	}
	if d.Text() != "ac" {
		t.Fatalf("got %q", d.Text())
	}
}

// TestDeleteBeforeInsert 删除先于插入到达仍收敛。
func TestDeleteBeforeInsert(t *testing.T) {
	d := NewDocument(1)
	ins := d.Insert(0, "x")[0]
	del := DeleteOp(OpID{Client: 2, Clock: 1}, ins.ID)

	fresh := NewDocument(3)
	if err := fresh.Apply(del); err != nil {
		t.Fatal(err)
	}
	if err := fresh.Apply(ins); err != nil {
		t.Fatal(err)
	}
	if fresh.Text() != "" {
		t.Fatalf("expected empty (deleted), got %q", fresh.Text())
	}
}
