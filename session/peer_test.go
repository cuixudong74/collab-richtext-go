package session

import (
	"testing"
	"time"

	"github.com/cuixudong74/collab-richtext-go/presence"
	"github.com/cuixudong74/collab-richtext-go/transport"
)

func testCfg() Config {
	return Config{
		HeartbeatInterval: 5 * time.Millisecond,
		PresenceTTL:       30 * time.Millisecond,
		ResyncInterval:    10 * time.Millisecond,
	}
}

// 可靠网络下的基本收敛。
func TestBasicConvergence(t *testing.T) {
	net := transport.NewNetwork(transport.Config{Seed: 1})
	defer net.Close()
	a := NewPeer("a", net, testCfg())
	b := NewPeer("b", net, testCfg())
	a.Start()
	b.Start()
	defer a.Close()
	defer b.Close()

	a.Insert(0, 'h')
	a.Insert(1, 'i')
	b.Insert(0, '>')
	net.Flush()
	if a.Text() != b.Text() {
		t.Fatalf("diverged: %q vs %q", a.Text(), b.Text())
	}
}

// 乱序 + 重复 + 丢包网络下，多端并发编辑最终收敛。
func TestFaultyNetworkConvergence(t *testing.T) {
	net := transport.NewNetwork(transport.Config{
		DropRate:   0.2,
		DupRate:    0.3,
		MinLatency: time.Millisecond,
		MaxLatency: 5 * time.Millisecond,
		Seed:       99,
	})
	defer net.Close()
	cfg := testCfg()
	peers := []*Peer{
		NewPeer("a", net, cfg),
		NewPeer("b", net, cfg),
		NewPeer("c", net, cfg),
	}
	for _, p := range peers {
		p.Start()
		defer p.Close()
	}
	// 各端并发插入
	for i, p := range peers {
		for j := 0; j < 5; j++ {
			p.Insert(p.Doc().Len(), rune('a'+i*5+j))
		}
	}
	peers[0].SetAttr(0, 3, "bold", "true")
	peers[1].Delete(0)
	net.Flush()
	// 周期重同步会自愈丢包；等待若干轮
	deadline := time.Now().Add(2 * time.Second)
	for {
		net.Flush()
		if peers[0].Text() == peers[1].Text() && peers[1].Text() == peers[2].Text() {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("no convergence: %q %q %q",
				peers[0].Text(), peers[1].Text(), peers[2].Text())
		}
		time.Sleep(20 * time.Millisecond)
	}
	// 属性也应一致
	for i := 0; i < peers[0].Doc().Len(); i++ {
		if !attrsEqual(peers[0].AttrsAt(i), peers[1].AttrsAt(i)) ||
			!attrsEqual(peers[1].AttrsAt(i), peers[2].AttrsAt(i)) {
			t.Fatalf("attrs diverged at %d", i)
		}
	}
}

func attrsEqual(x, y map[string]string) bool {
	if len(x) != len(y) {
		return false
	}
	for k, v := range x {
		if y[k] != v {
			return false
		}
	}
	return true
}

// 离线编辑 + 重连后补发合并。
func TestOfflineReconnect(t *testing.T) {
	net := transport.NewNetwork(transport.Config{Seed: 2})
	defer net.Close()
	cfg := testCfg()
	a := NewPeer("a", net, cfg)
	b := NewPeer("b", net, cfg)
	a.Start()
	b.Start()
	defer a.Close()
	defer b.Close()

	a.Insert(0, 'x')
	net.Flush()

	// b 离线编辑
	b.Disconnect()
	b.Insert(1, '1')
	b.Insert(2, '2')
	b.Delete(0)
	// a 在线继续编辑
	a.Insert(1, 'y')
	net.Flush()
	if b.Text() == a.Text() {
		t.Fatalf("should diverge while offline, both %q", a.Text())
	}
	// 重连后收敛
	b.Reconnect()
	net.Flush()
	deadline := time.Now().Add(time.Second)
	for a.Text() != b.Text() {
		if time.Now().After(deadline) {
			t.Fatalf("no convergence after reconnect: %q vs %q", a.Text(), b.Text())
		}
		time.Sleep(10 * time.Millisecond)
		net.Flush()
	}
	if b.Text() != "y12" && b.Text() != "12y" && b.Text() != "1y2" {
		// 内容字符集合必须正确（顺序由 CRDT 仲裁）
		t.Fatalf("unexpected merged text %q", b.Text())
	}
}

// 快照恢复 + 新加入者从快照对齐。
func TestSnapshotAndNewJoiner(t *testing.T) {
	net := transport.NewNetwork(transport.Config{Seed: 3})
	defer net.Close()
	cfg := testCfg()
	a := NewPeer("a", net, cfg)
	a.Start()
	defer a.Close()
	for i, ch := range "hello" {
		a.Insert(i, ch)
	}
	a.SetAttr(0, 5, "bold", "true")
	a.Compact() // 压缩历史

	// 模拟重启：快照持久化后恢复
	data, err := a.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	a2 := NewPeer("a", net, cfg)
	if err := a2.Restore(data); err != nil {
		t.Fatal(err)
	}
	if a2.Text() != "hello" || a2.AttrsAt(0)["bold"] != "true" {
		t.Fatalf("restore failed: %q %v", a2.Text(), a2.AttrsAt(0))
	}

	// 新加入者从“快照 + 增量”对齐
	b := NewPeer("b", net, cfg)
	b.Start()
	defer b.Close()
	a.Insert(5, '!') // 压缩后的增量
	net.Flush()
	deadline := time.Now().Add(time.Second)
	for b.Text() != "hello!" {
		if time.Now().After(deadline) {
			t.Fatalf("joiner not aligned: %q", b.Text())
		}
		time.Sleep(10 * time.Millisecond)
		net.Flush()
	}
	if b.AttrsAt(0)["bold"] != "true" {
		t.Fatalf("joiner lost attrs: %v", b.AttrsAt(0))
	}
}

// 压缩历史的一端与持有完整历史的一端继续收敛。
func TestCompactedPeerStillConverges(t *testing.T) {
	net := transport.NewNetwork(transport.Config{Seed: 4})
	defer net.Close()
	cfg := testCfg()
	a := NewPeer("a", net, cfg)
	b := NewPeer("b", net, cfg)
	a.Start()
	b.Start()
	defer a.Close()
	defer b.Close()

	a.Insert(0, 'a')
	net.Flush()
	a.Compact() // a 压缩历史；b 持有完整历史

	b.Insert(1, 'b')
	a.Insert(1, 'X')
	net.Flush()
	deadline := time.Now().Add(time.Second)
	for a.Text() != b.Text() {
		if time.Now().After(deadline) {
			t.Fatalf("diverged: %q vs %q", a.Text(), b.Text())
		}
		time.Sleep(10 * time.Millisecond)
		net.Flush()
	}
}

// 在线状态：加入可见、离开清除、异常掉线超时清理、不进快照。
func TestPresenceLifecycle(t *testing.T) {
	net := transport.NewNetwork(transport.Config{Seed: 5})
	defer net.Close()
	cfg := testCfg()
	a := NewPeer("a", net, cfg)
	b := NewPeer("b", net, cfg)
	a.Start()
	b.Start()
	defer a.Close()

	a.UpdatePresence(presence.State{Cursor: 3, SelStart: 1, SelEnd: 4})
	net.Flush()
	deadline := time.Now().Add(time.Second)
	for {
		st, ok := b.Presence()["a"]
		if ok && st.Cursor == 3 && st.SelEnd == 4 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("presence not propagated: %v", b.Presence())
		}
		time.Sleep(5 * time.Millisecond)
		net.Flush()
	}

	// 快照不含在线状态
	data, _ := b.Snapshot()
	c := NewPeer("c", net, cfg)
	c.Restore(data)
	if len(c.Presence()) != 0 {
		t.Fatalf("presence leaked into snapshot: %v", c.Presence())
	}

	// 正常离开：bye 广播后立即清除
	a.Close()
	net.Flush()
	if _, ok := b.Presence()["a"]; ok {
		t.Fatal("bye should remove presence")
	}

	// 异常掉线：无 bye，TTL 超时后自动清理
	b.UpdatePresence(presence.State{Cursor: 1})
	c2 := NewPeer("c2", net, cfg)
	c2.Start()
	c2.UpdatePresence(presence.State{Cursor: 9})
	net.Flush()
	time.Sleep(20 * time.Millisecond)
	net.Flush()
	if _, ok := b.Presence()["c2"]; !ok {
		t.Fatalf("c2 presence expected: %v", b.Presence())
	}
	c2.Disconnect() // 异常掉线，不 Close（无 bye）
	deadline = time.Now().Add(2 * time.Second)
	for {
		if _, ok := b.Presence()["c2"]; !ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("stale presence not expired: %v", b.Presence())
		}
		time.Sleep(20 * time.Millisecond)
	}
	c2.Close()
	b.Close()
}

// 固定种子 + 固定操作脚本的确定性重放。
func TestDeterministicReplay(t *testing.T) {
	run := func() string {
		net := transport.NewNetwork(transport.Config{
			DropRate: 0.1, DupRate: 0.2,
			MinLatency: time.Microsecond, MaxLatency: 50 * time.Microsecond,
			Seed: 42,
		})
		defer net.Close()
		cfg := Config{} // 无心跳无周期重同步，排除时间因素
		a := NewPeer("a", net, cfg)
		b := NewPeer("b", net, cfg)
		a.Start()
		b.Start()
		a.Insert(0, 'a')
		b.Insert(0, 'b')
		a.SetAttr(0, 1, "bold", "true")
		b.Delete(0)
		a.Insert(0, 'c')
		net.Flush()
		// 丢包靠显式重同步补齐
		for i := 0; i < 5; i++ {
			a.Reconnect()
			b.Reconnect()
			net.Flush()
		}
		res := a.Text()
		a.Close()
		b.Close()
		return res
	}
	first := run()
	for i := 0; i < 3; i++ {
		if got := run(); got != first {
			t.Fatalf("nondeterministic: %q vs %q", first, got)
		}
	}
}
