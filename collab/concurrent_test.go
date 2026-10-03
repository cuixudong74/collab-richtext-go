package collab

import (
	"math/rand"
	"sync"
	"testing"
	"time"
)

// TestConcurrentReadWrite 验证本地读取、远端操作应用、快照写入同时发生时
// 不读到半截状态，也不存在数据竞争。
func TestConcurrentReadWrite(t *testing.T) {
	d := NewDocument(1)
	d.Insert(0, "hello world")

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// 并发读取
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					_ = d.Text()
					_ = d.RichText()
					_ = d.VV()
				}
			}
		}()
	}

	// 并发写入
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(n)))
			for {
				select {
				case <-stop:
					return
				default:
					pos := rng.Intn(len([]rune(d.Text())) + 1)
					d.Insert(pos, "x")
					time.Sleep(time.Microsecond)
				}
			}
		}(i)
	}

	// 快照应入
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					snap := d.Snapshot()
					d2 := NewDocument(2)
					d2.Restore(snap)
					_ = d2.Text()
				}
			}
		}()
	}

	time.Sleep(200 * time.Millisecond)
	close(stop)
	wg.Wait()
}

// TestDeterministicReplay 验证固定操作集合与固定随机种子下的确定性重放。
func TestDeterministicReplay(t *testing.T) {
	// 生成固定操作集合
	seed := int64(999)
	rng := rand.New(rand.NewSource(seed))
	docs := []*Document{NewDocument(1), NewDocument(2), NewDocument(3)}
	var allOps []*Op
	for i := 0; i < 50; i++ {
		di := rng.Intn(3)
		doc := docs[di]
		pos := rng.Intn(len([]rune(doc.Text())) + 1)
		ch := rune('a' + rng.Intn(26))
		ops := doc.Insert(pos, string(ch))
		allOps = append(allOps, ops...)
	}

	// 用固定种子打乱顺序
	shuffleSeed := int64(12345)
	order := make([]*Op, len(allOps))
	copy(order, allOps)
	rng2 := rand.New(rand.NewSource(shuffleSeed))
	rng2.Shuffle(len(order), func(i, j int) { order[i], order[j] = order[j], order[i] })

	// 重放两次，结果应完全一致
	replay := func() string {
		d := NewDocument(1)
		for _, op := range order {
			d.Apply(op)
		}
		return d.Text()
	}
	r1 := replay()
	r2 := replay()
	if r1 != r2 {
		t.Fatalf("replay not deterministic: %q vs %q", r1, r2)
	}

	// 与全量应用到独立文档的结果收敛
	d := NewDocument(1)
	for _, op := range allOps {
		d.Apply(op)
	}
	if d.Text() != r1 {
		t.Fatalf("replay diverges from full apply: %q vs %q", d.Text(), r1)
	}
	t.Logf("deterministic replay: %q", r1)
}

// TestLossRecovery 验证丢包后通过重连同步恢复。
func TestLossRecovery(t *testing.T) {
	cfg := DefaultTransportConfig()
	cfg.LossRate = 0.3 // 30% 丢包
	cfg.Seed = 777
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

	// p1 发送多个操作（会丢包）
	for i := 0; i < 10; i++ {
		p1.Insert(i, "x")
	}
	time.Sleep(200 * time.Millisecond)

	// 多次触发同步补齐（同步消息本身也可能丢）
	for i := 0; i < 5; i++ {
		p1.sendSyncRequest(2)
		p2.sendSyncRequest(1)
		time.Sleep(100 * time.Millisecond)
	}
	transport.WaitQuiescent()

	if p1.Doc().Text() != p2.Doc().Text() {
		t.Fatalf("diverge after loss recovery: p1=%q p2=%q", p1.Doc().Text(), p2.Doc().Text())
	}
	t.Logf("recovered text: %q", p1.Doc().Text())
}

// TestOfflineEditing 验证离线编辑后重新加入并合并。
func TestOfflineEditing(t *testing.T) {
	cfg := DefaultTransportConfig()
	transport := NewLoopbackTransport(cfg)

	// 初始：p1 和 p2 同步
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

	// p2 离线
	transport.Disconnect(2, 1)
	transport.Disconnect(1, 2)

	// p2 离线编辑
	p2.Insert(5, " world")
	time.Sleep(50 * time.Millisecond)

	// p1 也在线编辑
	p1.Insert(5, "!")
	time.Sleep(50 * time.Millisecond)

	// 重连
	transport.Reconnect(2, 1)
	transport.Reconnect(1, 2)

	// 双向同步
	p1.sendSyncRequest(2)
	p2.sendSyncRequest(1)
	time.Sleep(200 * time.Millisecond)
	transport.WaitQuiescent()

	if p1.Doc().Text() != p2.Doc().Text() {
		t.Fatalf("diverge after offline editing: p1=%q p2=%q", p1.Doc().Text(), p2.Doc().Text())
	}
	t.Logf("merged text: %q", p1.Doc().Text())
}
