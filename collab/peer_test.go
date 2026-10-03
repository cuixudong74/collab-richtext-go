package collab

import (
	"testing"
	"time"
)

// TestPeerSync 验证两个参与者通过回环传输同步文档。
func TestPeerSync(t *testing.T) {
	cfg := DefaultTransportConfig()
	transport := NewLoopbackTransport(cfg)

	p1 := NewPeer(1, "alice", transport)
	p2 := NewPeer(2, "bob", transport)
	p1.Start()
	p2.Start()
	defer p1.Close()
	defer p2.Close()

	p1.peers[2] = p2.presence
	p2.peers[1] = p1.presence
	p1.Join()
	p2.Join()

	p1.Insert(0, "hello")
	time.Sleep(100 * time.Millisecond)

	if p2.Doc().Text() != "hello" {
		t.Fatalf("p2 text: %q", p2.Doc().Text())
	}

	p2.Insert(5, " world")
	time.Sleep(100 * time.Millisecond)

	if p1.Doc().Text() != p2.Doc().Text() {
		t.Fatalf("diverge: p1=%q p2=%q", p1.Doc().Text(), p2.Doc().Text())
	}
	t.Logf("synced text: %q", p1.Doc().Text())
}

// TestPeerSyncWithReorder 验证乱序传输下仍收敛。
func TestPeerSyncWithReorder(t *testing.T) {
	cfg := DefaultTransportConfig()
	cfg.ReorderWindow = 20
	cfg.Seed = 123
	transport := NewLoopbackTransport(cfg)

	p1 := NewPeer(1, "alice", transport)
	p2 := NewPeer(2, "bob", transport)
	p1.Start()
	p2.Start()
	defer p1.Close()
	defer p2.Close()

	p1.peers[2] = p2.presence
	p2.peers[1] = p1.presence
	p1.Join()
	p2.Join()

	p1.Insert(0, "abc")
	p2.Insert(0, "xyz")
	time.Sleep(300 * time.Millisecond)
	transport.WaitQuiescent()

	if p1.Doc().Text() != p2.Doc().Text() {
		t.Fatalf("diverge with reorder: p1=%q p2=%q", p1.Doc().Text(), p2.Doc().Text())
	}
	t.Logf("converged with reorder: %q", p1.Doc().Text())
}

// TestPeerSyncWithDuplicate 验证重复投递不产生二次影响。
func TestPeerSyncWithDuplicate(t *testing.T) {
	cfg := DefaultTransportConfig()
	cfg.DuplicateRate = 1.0
	cfg.Seed = 456
	transport := NewLoopbackTransport(cfg)

	p1 := NewPeer(1, "alice", transport)
	p2 := NewPeer(2, "bob", transport)
	p1.Start()
	p2.Start()
	defer p1.Close()
	defer p2.Close()

	p1.peers[2] = p2.presence
	p2.peers[1] = p1.presence
	p1.Join()
	p2.Join()

	p1.Insert(0, "hello")
	time.Sleep(200 * time.Millisecond)
	transport.WaitQuiescent()

	if p2.Doc().Text() != "hello" {
		t.Fatalf("p2 text: %q", p2.Doc().Text())
	}
}

// TestPeerReconnect 验证断线重连后操作补发并收敛。
func TestPeerReconnect(t *testing.T) {
	cfg := DefaultTransportConfig()
	cfg.ReconnectDelay = 50 * time.Millisecond
	transport := NewLoopbackTransport(cfg)

	p1 := NewPeer(1, "alice", transport)
	p2 := NewPeer(2, "bob", transport)
	p1.Start()
	p2.Start()
	defer p1.Close()
	defer p2.Close()

	p1.peers[2] = p2.presence
	p2.peers[1] = p1.presence
	p1.Join()
	p2.Join()

	// 断线
	transport.Disconnect(1, 2)
	transport.Disconnect(2, 1)

	// 断线期间 p1 本地编辑
	p1.Insert(0, "offline")
	time.Sleep(50 * time.Millisecond)

	// 重连
	transport.Reconnect(1, 2)
	transport.Reconnect(2, 1)

	// 双向触发同步
	p1.sendSyncRequest(2)
	p2.sendSyncRequest(1)
	time.Sleep(200 * time.Millisecond)
	transport.WaitQuiescent()

	if p2.Doc().Text() != "offline" {
		t.Fatalf("p2 text after reconnect: %q", p2.Doc().Text())
	}
	if p1.Doc().Text() != p2.Doc().Text() {
		t.Fatalf("diverge after reconnect: p1=%q p2=%q", p1.Doc().Text(), p2.Doc().Text())
	}
}

// TestPresence 验证在线状态广播与清理。
func TestPresence(t *testing.T) {
	cfg := DefaultTransportConfig()
	transport := NewLoopbackTransport(cfg)

	p1 := NewPeer(1, "alice", transport)
	p2 := NewPeer(2, "bob", transport)
	p1.Start()
	p2.Start()
	defer p1.Close()
	defer p2.Close()

	p1.peers[2] = p2.presence
	p2.peers[1] = p1.presence
	p1.Join()
	p2.Join()

	p1.UpdateCursor(Anchor{Char: OpID{}, Assoc: AssocAfter})
	time.Sleep(100 * time.Millisecond)

	peers := p2.Peers()
	if peers[1] == nil {
		t.Fatal("p2 should see p1's presence")
	}
	t.Logf("p2 sees p1: %+v", peers[1])

	p1.Leave()
	time.Sleep(100 * time.Millisecond)

	peers = p2.Peers()
	if peers[1] != nil {
		t.Fatal("p2 should not see p1 after leave")
	}
}

// TestPresenceTimeout 验证异常掉线后在线状态自动清理。
func TestPresenceTimeout(t *testing.T) {
	cfg := DefaultTransportConfig()
	transport := NewLoopbackTransport(cfg)

	p1 := NewPeer(1, "alice", transport)
	p2 := NewPeer(2, "bob", transport)
	p1.Start()
	p2.Start()
	defer p1.Close()
	defer p2.Close()

	p1.peers[2] = p2.presence
	p2.peers[1] = p1.presence
	p1.Join()
	p2.Join()

	time.Sleep(50 * time.Millisecond)

	// p2 看到 p1
	if p2.Peers()[1] == nil {
		t.Fatal("p2 should see p1 before timeout")
	}

	// 手动将 p1 的 LastSeen 设为很久以前（模拟异常掉线）
	p2.mu.Lock()
	p2.peers[1].LastSeen = time.Now().Add(-10 * time.Second)
	p2.mu.Unlock()

	// 清理超时参与者
	removed := p2.CleanupStalePeers(1 * time.Second)
	if len(removed) != 1 || removed[0] != 1 {
		t.Fatalf("expected p1 to be removed, got %v", removed)
	}

	if p2.Peers()[1] != nil {
		t.Fatal("p1 should be removed after timeout")
	}
}

// TestRetryPending 验证未确认操作的重试机制。
func TestRetryPending(t *testing.T) {
	cfg := DefaultTransportConfig()
	cfg.AckTimeout = 50 * time.Millisecond
	cfg.MaxRetries = 3
	transport := NewLoopbackTransport(cfg)

	p1 := NewPeer(1, "alice", transport, cfg)
	p2 := NewPeer(2, "bob", transport, cfg)
	p1.Start()
	p2.Start()
	defer p1.Close()
	defer p2.Close()

	p1.peers[2] = p2.presence
	p2.peers[1] = p1.presence
	p1.Join()
	p2.Join()

	// p1 发送操作
	p1.Insert(0, "hello")
	time.Sleep(200 * time.Millisecond)

	// p2 应该收到（即使有重试）
	if p2.Doc().Text() != "hello" {
		t.Fatalf("p2 text: %q", p2.Doc().Text())
	}

	// 确认后 pendingAck 应为空
	p1.mu.Lock()
	pending := len(p1.pendingAck)
	p1.mu.Unlock()
	if pending != 0 {
		t.Fatalf("expected 0 pending acks, got %d", pending)
	}
}

// TestUpdateSelection 验证选区更新广播。
func TestUpdateSelection(t *testing.T) {
	cfg := DefaultTransportConfig()
	transport := NewLoopbackTransport(cfg)

	p1 := NewPeer(1, "alice", transport)
	p2 := NewPeer(2, "bob", transport)
	p1.Start()
	p2.Start()
	defer p1.Close()
	defer p2.Close()

	p1.peers[2] = p2.presence
	p2.peers[1] = p1.presence
	p1.Join()
	p2.Join()

	start := Anchor{Char: OpID{}, Assoc: AssocAfter}
	end := Anchor{Char: OpID{}, Assoc: AssocBefore}
	p1.UpdateSelection(start, end)
	time.Sleep(100 * time.Millisecond)

	peers := p2.Peers()
	if peers[1] == nil {
		t.Fatal("p2 should see p1")
	}
	if peers[1].SelStart != start || peers[1].SelEnd != end {
		t.Fatalf("selection not updated: %+v", peers[1])
	}
}
