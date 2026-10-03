package session

import (
	"sync"
	"testing"
	"time"

	"github.com/cuixudong74/collab-richtext-go/transport"
)

// 远端应用、本地读取、快照写入并发发生：
// 不得读到半截状态，不得有数据竞争（需 -race 运行）。
func TestConcurrentReadWriteSnapshot(t *testing.T) {
	net := transport.NewNetwork(transport.Config{
		DupRate: 0.2, MaxLatency: time.Millisecond, Seed: 8,
	})
	defer net.Close()
	cfg := testCfg()
	a := NewPeer("a", net, cfg)
	b := NewPeer("b", net, cfg)
	a.Start()
	b.Start()
	defer a.Close()
	defer b.Close()

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// 写入者
	for i, p := range []*Peer{a, b} {
		p := p
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				p.Insert(p.Doc().Len(), rune('a'+i))
				if j%10 == 0 {
					p.SetAttr(0, 1, "bold", "true")
				}
			}
		}()
	}
	// 读取者（由 stop 控制退出，不参与 wg）
	var readerWg sync.WaitGroup
	readerWg.Add(1)
	go func() {
		defer readerWg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			_ = a.Text()
			if a.Doc().Len() > 0 {
				_ = a.AttrsAt(0)
			}
			_ = a.Presence()
		}
	}()
	// 快照者
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 20; i++ {
			data, err := a.Snapshot()
			if err != nil {
				t.Error(err)
				return
			}
			c := NewPeer("tmp", net, cfg)
			if err := c.Restore(data); err != nil {
				t.Error(err)
				return
			}
		}
	}()
	wg.Wait()
	close(stop)
	readerWg.Wait()
	net.Flush()
	deadline := time.Now().Add(2 * time.Second)
	for a.Text() != b.Text() {
		if time.Now().After(deadline) {
			t.Fatalf("diverged: %q vs %q", a.Text(), b.Text())
		}
		time.Sleep(10 * time.Millisecond)
		net.Flush()
	}
	if n := a.Doc().Len(); n != 100 {
		t.Fatalf("expected 100 chars, got %d", n)
	}
}
