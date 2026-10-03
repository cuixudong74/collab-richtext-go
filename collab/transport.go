package collab

import (
	"math/rand"
	"sync"
	"time"
)

// TransportConfig 描述回环传输的模拟参数。
type TransportConfig struct {
	LossRate        float64       // 丢包率 [0,1]
	DuplicateRate   float64       // 重复投递率 [0,1]
	ReorderWindow   int           // 乱序窗口（消息最大乱序步长，0 表示不乱序）
	ReconnectDelay  time.Duration // 断线重连延迟
	AckTimeout      time.Duration // 确认超时
	MaxRetries      int           // 最大重试次数
	Seed            int64         // 随机种子（确定性重放）
}

// DefaultTransportConfig 返回一组默认可调参数。
func DefaultTransportConfig() TransportConfig {
	return TransportConfig{
		LossRate:       0.0,
		DuplicateRate:  0.0,
		ReorderWindow:  0,
		ReconnectDelay: 50 * time.Millisecond,
		AckTimeout:     200 * time.Millisecond,
		MaxRetries:     5,
		Seed:           42,
	}
}

// Message 是参与者之间交换的消息。
type Message struct {
	From  ClientID
	To    ClientID
	Kind  MsgKind
	MsgID uint64
	Op    *Op
	Ops   []*Op
	VV    VersionVector
	Pres  *Presence
}

// MsgKind 标识消息类型。
type MsgKind uint8

const (
	MsgOp MsgKind = iota
	MsgAck
	MsgPresence
	MsgSyncRequest
	MsgSyncData
	MsgJoin
	MsgLeave
)

// Transport 抽象参与者之间的通信能力。
type Transport interface {
	Send(msg *Message) error
	Close() error
}

// LoopbackTransport 是本地回环传输实现，用内存通道模拟网络，
// 可配置乱序、重复、丢包与断线重连，不依赖任何外部服务。
type LoopbackTransport struct {
	mu           sync.Mutex
	inboxes      map[ClientID]chan *Message
	config       TransportConfig
	rng          *rand.Rand
	disconnected map[ClientID]map[ClientID]bool // from -> to 单向断线
	closed       bool
	wg           sync.WaitGroup
}

// NewLoopbackTransport 创建回环传输。
func NewLoopbackTransport(cfg TransportConfig) *LoopbackTransport {
	return &LoopbackTransport{
		inboxes:      make(map[ClientID]chan *Message),
		config:       cfg,
		rng:          rand.New(rand.NewSource(cfg.Seed)),
		disconnected: make(map[ClientID]map[ClientID]bool),
	}
}

// Register 注册一个参与者，返回其接收通道。
func (t *LoopbackTransport) Register(peer ClientID) <-chan *Message {
	t.mu.Lock()
	defer t.mu.Unlock()
	ch := make(chan *Message, 256)
	t.inboxes[peer] = ch
	return ch
}

// Disconnect 模拟 from 到 to 的单向断线。
func (t *LoopbackTransport) Disconnect(from, to ClientID) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.disconnected[from] == nil {
		t.disconnected[from] = make(map[ClientID]bool)
	}
	t.disconnected[from][to] = true
}

// Reconnect 恢复 from 到 to 的单向断线。
func (t *LoopbackTransport) Reconnect(from, to ClientID) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if m := t.disconnected[from]; m != nil {
		delete(m, to)
	}
}

// IsDisconnected 判断 from 到 to 是否断线。
func (t *LoopbackTransport) IsDisconnected(from, to ClientID) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.disconnected[from][to]
}

// Send 发送一条消息，应用丢包/重复/乱序模拟。
func (t *LoopbackTransport) Send(msg *Message) error {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return ErrTransportClosed
	}
	if t.disconnected[msg.From][msg.To] {
		t.mu.Unlock()
		return nil // 断线丢包
	}
	// 丢包
	if t.rng.Float64() < t.config.LossRate {
		t.mu.Unlock()
		return nil
	}
	// 重复
	dup := 1
	if t.rng.Float64() < t.config.DuplicateRate {
		dup = 2
	}
	// 乱序延迟
	var delay time.Duration
	if t.config.ReorderWindow > 0 {
		delay = time.Duration(t.rng.Intn(t.config.ReorderWindow)) * time.Millisecond
	}
	inbox := t.inboxes[msg.To]
	t.mu.Unlock()

	if inbox == nil {
		return nil
	}

	for i := 0; i < dup; i++ {
		m := *msg // 副本
		if delay > 0 {
			t.wg.Add(1)
			go func(m *Message, d time.Duration) {
				defer t.wg.Done()
				time.Sleep(d)
				select {
				case inbox <- m:
				default:
				}
			}(&m, delay)
		} else {
			select {
			case inbox <- &m:
			default:
			}
		}
	}
	return nil
}

// Recv 返回指定参与者的接收通道。
func (t *LoopbackTransport) Recv(peer ClientID) <-chan *Message {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.inboxes[peer]
}

// Close 关闭传输。
func (t *LoopbackTransport) Close() error {
	t.mu.Lock()
	t.closed = true
	for _, ch := range t.inboxes {
		close(ch)
	}
	t.mu.Unlock()
	t.wg.Wait()
	return nil
}

// WaitQuiescent 等待所有延迟消息投递完成。
func (t *LoopbackTransport) WaitQuiescent() {
	t.wg.Wait()
}
