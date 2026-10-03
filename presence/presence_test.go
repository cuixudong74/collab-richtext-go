package presence

import (
	"testing"
	"time"
)

func TestUpdateAndList(t *testing.T) {
	tr := NewTracker(time.Minute)
	tr.Update("a", State{Cursor: 1, SelStart: 1, SelEnd: 3})
	tr.Update("b", State{Cursor: 5})
	got := tr.List()
	if len(got) != 2 || got["a"].SelEnd != 3 || got["b"].Cursor != 5 {
		t.Fatalf("unexpected: %v", got)
	}
	tr.Remove("a")
	if _, ok := tr.List()["a"]; ok {
		t.Fatal("remove failed")
	}
}

func TestExpire(t *testing.T) {
	now := time.Unix(1000, 0)
	cur := now
	tr := NewTracker(10 * time.Second)
	tr.SetClock(func() time.Time { return cur })
	tr.Update("a", State{Cursor: 1})
	cur = now.Add(5 * time.Second)
	tr.Update("b", State{Cursor: 2})
	cur = now.Add(11 * time.Second)
	gone := tr.Expire()
	if len(gone) != 1 || gone[0] != "a" {
		t.Fatalf("expired %v", gone)
	}
	if _, ok := tr.List()["b"]; !ok {
		t.Fatal("b should survive")
	}
	// 异常掉线（无 bye）也会被清理
	cur = now.Add(21 * time.Second)
	if gone := tr.Expire(); len(gone) != 1 || gone[0] != "b" {
		t.Fatalf("expired %v", gone)
	}
}
