// Package presence 管理参与者的临时在线状态（光标/选区）。
// 这些状态不写入文档历史，也不进入快照；异常掉线的参与者
// 会在 TTL 超时后被自动清理。
package presence

import (
	"sync"
	"time"
)

// State 是一个参与者的光标与选区（可见字符位置）。
type State struct {
	Cursor   int `json:"cursor"`
	SelStart int `json:"sel_start"`
	SelEnd   int `json:"sel_end"`
}

// Info 携带最后心跳时间，用于超时清理。
type Info struct {
	State     State
	UpdatedAt time.Time
}

// Tracker 跟踪所有参与者的在线状态，并发安全。
type Tracker struct {
	mu    sync.RWMutex
	peers map[string]Info
	ttl   time.Duration
	now   func() time.Time // 可注入时钟，便于确定性测试
}

func NewTracker(ttl time.Duration) *Tracker {
	return &Tracker{
		peers: make(map[string]Info),
		ttl:   ttl,
		now:   time.Now,
	}
}

// SetClock 注入时钟（测试用）。
func (t *Tracker) SetClock(now func() time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.now = now
}

// Update 更新某参与者状态并刷新心跳。
func (t *Tracker) Update(peer string, s State) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.peers[peer] = Info{State: s, UpdatedAt: t.now()}
}

// Remove 参与者离开时立即移除。
func (t *Tracker) Remove(peer string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.peers, peer)
}

// Expire 清理超时未心跳的参与者，返回被清理者列表。
func (t *Tracker) Expire() []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	var gone []string
	deadline := t.now().Add(-t.ttl)
	for id, info := range t.peers {
		if info.UpdatedAt.Before(deadline) {
			delete(t.peers, id)
			gone = append(gone, id)
		}
	}
	return gone
}

// List 返回当前所有在线参与者状态的副本。
func (t *Tracker) List() map[string]State {
	t.mu.RLock()
	defer t.mu.RUnlock()
	out := make(map[string]State, len(t.peers))
	for id, info := range t.peers {
		out[id] = info.State
	}
	return out
}
