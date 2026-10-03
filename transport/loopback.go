// Package transport 提供本地回环传输层，可模拟乱序、重复、
// 丢包与断线重连，不依赖任何外部服务。
// 传输层本身是不可靠的：可靠性（不丢操作、不重复应用）
// 由上层会话通过版本向量与补发机制保证。
package transport

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// Config 控制故障注入，全部可调；Seed 固定后决策序列确定。
type Config struct {
	DropRate   float64       // 每条消息被丢弃的概率 [0,1]
	DupRate    float64       // 每条消息被额外复制一次的概率 [0,1]
	MinLatency time.Duration // 最小投递延迟
	MaxLatency time.Duration // 最大投递延迟（>Min 时产生乱序）
	Seed       int64         // 随机种子，固定后可确定性重放
}

// Message 是传输层消息。
type Message struct {
	From string
	To   string // 空串表示广播
	Kind string // "op" | "hello" | "sync" | "snapshot" | "presence" | "bye"
	Data []byte
}

// Handler 接收投递来的消息。
type Handler func(Message)

// Network 是回环网络，所有连接在其上互联。
type Network struct {
	mu     sync.Mutex
	cfg    Config
	rnd    *rand.Rand
	conns  map[string]*Conn
	closed bool

	pendMu   sync.Mutex
	pendCond sync.Cond // 在 pend 降为 0 时广播
	pend     int64     // 在途投递数
}

func NewNetwork(cfg Config) *Network {
	if cfg.MaxLatency < cfg.MinLatency {
		cfg.MaxLatency = cfg.MinLatency
	}
	n := &Network{
		cfg:   cfg,
		rnd:   rand.New(rand.NewSource(cfg.Seed)),
		conns: make(map[string]*Conn),
	}
	n.pendCond.L = &n.pendMu
	return n
}

// Connect 以 id 接入网络；重复接入同 id 会替换旧连接。
func (n *Network) Connect(id string, h Handler) *Conn {
	n.mu.Lock()
	defer n.mu.Unlock()
	c := &Conn{id: id, net: n, handler: h}
	c.connected.Store(true)
	n.conns[id] = c
	return c
}

// Peers 返回当前已接入的节点 id（不含 except）。
func (n *Network) Peers(except string) []string {
	n.mu.Lock()
	defer n.mu.Unlock()
	var out []string
	for id, c := range n.conns {
		if id != except && c.connected.Load() {
			out = append(out, id)
		}
	}
	return out
}

// Flush 阻塞直到所有已调度投递完成（测试辅助）。
func (n *Network) Flush() {
	n.pendMu.Lock()
	for n.pend > 0 {
		n.pendCond.Wait()
	}
	n.pendMu.Unlock()
}

func (n *Network) pendAdd(delta int64) {
	n.pendMu.Lock()
	n.pend += delta
	if n.pend == 0 {
		n.pendCond.Broadcast()
	}
	n.pendMu.Unlock()
}

// Close 停止网络，丢弃未完成的投递。
func (n *Network) Close() {
	n.mu.Lock()
	n.closed = true
	n.mu.Unlock()
	n.Flush()
}

// deliver 按故障注入策略调度投递。
func (n *Network) deliver(m Message) {
	n.mu.Lock()
	if n.closed {
		n.mu.Unlock()
		return
	}
	if n.rnd.Float64() < n.cfg.DropRate {
		n.mu.Unlock()
		return // 丢包
	}
	times := 1
	if n.rnd.Float64() < n.cfg.DupRate {
		times = 2 // 重复
	}
	delay := n.cfg.MinLatency
	if span := n.cfg.MaxLatency - n.cfg.MinLatency; span > 0 {
		delay += time.Duration(n.rnd.Int63n(int64(span)))
	}
	n.pendAdd(int64(times))
	n.mu.Unlock()
	for i := 0; i < times; i++ {
		go func() {
			defer n.pendAdd(-1)
			time.Sleep(delay)
			n.dispatch(m)
		}()
	}
}

// dispatch 在投递 goroutine 中同步调用处理器（受 inflight 跟踪，
// Flush 返回时处理器必然已执行完毕）；锁内只收集目标，避免重入死锁。
func (n *Network) dispatch(m Message) {
	n.mu.Lock()
	if n.closed {
		n.mu.Unlock()
		return
	}
	var handlers []Handler
	if m.To != "" {
		if c, ok := n.conns[m.To]; ok && c.connected.Load() {
			handlers = append(handlers, c.handler)
		}
	} else {
		for id, c := range n.conns {
			if id != m.From && c.connected.Load() {
				handlers = append(handlers, c.handler)
			}
		}
	}
	n.mu.Unlock()
	for _, h := range handlers {
		h(m)
	}
}

// Conn 是一条到网络的连接，可断开与重连。
type Conn struct {
	id      string
	net     *Network
	handler Handler

	connected atomic.Bool

	// OnReconnect 在重连成功后触发（用于发起状态同步）。
	OnReconnect func()
	// OnDisconnect 在断线时触发。
	OnDisconnect func()
}

func (c *Conn) ID() string { return c.id }

// Send 发送消息；To 为空串表示广播。断线期间发送的消息直接丢失。
func (c *Conn) Send(to string, m Message) {
	if !c.connected.Load() {
		return
	}
	m.From = c.id
	m.To = to
	c.net.deliver(m)
}

// Broadcast 向所有其他在线节点发送消息。
func (c *Conn) Broadcast(m Message) { c.Send("", m) }

// Disconnect 模拟断线。
func (c *Conn) Disconnect() {
	if c.connected.Swap(false) && c.OnDisconnect != nil {
		c.OnDisconnect()
	}
}

// Reconnect 恢复连接。
func (c *Conn) Reconnect() {
	if !c.connected.Swap(true) && c.OnReconnect != nil {
		c.OnReconnect()
	}
}

// Connected 报告当前是否在线。
func (c *Conn) Connected() bool { return c.connected.Load() }
