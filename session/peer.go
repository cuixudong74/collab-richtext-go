// Package session 将 CRDT 文档、传输层与在线状态组合成一个
// 可离线、可重连的协同编辑端点。
//
// 同步协议（全部为传输层消息）：
//   - sync-req：携带本地版本向量，请求对方补发缺失历史；
//     广播且 WantAck=true 时对方会回发自己的 sync-req，实现双向对齐。
//   - sync：携带一段操作区间，接收方幂等应用。
//   - snapshot：请求的历史已被压缩时，以快照对齐。
//   - op：实时操作广播；发现序号缺口时主动向来源发 sync-req 补齐。
//   - presence / bye：临时在线状态，不进历史、不进快照。
package session

import (
	"encoding/json"
	"sync"
	"time"

	"github.com/cuixudong74/collab-richtext-go/crdt"
	"github.com/cuixudong74/collab-richtext-go/presence"
	"github.com/cuixudong74/collab-richtext-go/transport"
)

// Config 控制会话的心跳与重试策略，均可调。
type Config struct {
	HeartbeatInterval time.Duration // 在线状态心跳间隔；0 关闭心跳
	PresenceTTL       time.Duration // 在线状态超时；0 表示不超时
	ResyncInterval    time.Duration // 周期性重同步间隔（丢包自愈）；0 关闭
}

type syncReq struct {
	Vector  crdt.VersionVector `json:"vector"`
	WantAck bool               `json:"want_ack"`
}

type syncMsg struct {
	Ops []crdt.Op `json:"ops"`
}

type opMsg struct {
	Op crdt.Op `json:"op"`
}

type snapshotMsg struct {
	Snap crdt.Snapshot `json:"snap"`
}

type presenceMsg struct {
	State presence.State `json:"state"`
}

// Peer 是一个协同编辑参与者。
type Peer struct {
	id  string
	net *transport.Network
	cfg Config

	mu       sync.Mutex
	doc      *crdt.Document
	localLog []crdt.Op // 本地产生的全部操作（快照恢复后重放用）
	pres     *presence.Tracker
	self     presence.State
	conn     *transport.Conn
	started  bool
	closed   bool
	stop     chan struct{}
	loopDone chan struct{}
}

func NewPeer(id string, net *transport.Network, cfg Config) *Peer {
	return &Peer{
		id:       id,
		net:      net,
		cfg:      cfg,
		doc:      crdt.NewDocument(crdt.SiteID(id)),
		pres:     presence.NewTracker(cfg.PresenceTTL),
		stop:     make(chan struct{}),
		loopDone: make(chan struct{}),
	}
}

// Start 接入网络：注册消息处理器并发起首次同步。
func (p *Peer) Start() {
	p.mu.Lock()
	if p.started || p.closed {
		p.mu.Unlock()
		return
	}
	p.started = true
	p.conn = p.net.Connect(p.id, p.handle)
	p.conn.OnReconnect = func() { p.announce() }
	p.mu.Unlock()
	p.announce()
	go func() {
		defer close(p.loopDone)
		p.loop()
	}()
}

// Close 离开会话：广播 bye 并断开连接。
func (p *Peer) Close() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	conn := p.conn
	p.mu.Unlock()
	if conn != nil {
		conn.Broadcast(transport.Message{Kind: "bye"})
		conn.Disconnect()
	}
	close(p.stop)
	if p.started {
		<-p.loopDone // 等待心跳 goroutine 退出，避免泄漏到测试之后
	}
}

// announce 广播 sync-req（连接建立或重连时调用），触发双向对齐。
func (p *Peer) announce() {
	p.mu.Lock()
	conn := p.conn
	v := p.doc.Version()
	p.mu.Unlock()
	if conn == nil {
		return
	}
	data, _ := json.Marshal(syncReq{Vector: v, WantAck: true})
	conn.Broadcast(transport.Message{Kind: "sync-req", Data: data})
}

// --- 消息处理 ---

func (p *Peer) handle(m transport.Message) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return
	}
	switch m.Kind {
	case "op":
		var msg opMsg
		if json.Unmarshal(m.Data, &msg) != nil {
			return
		}
		if p.doc.Version().Covers(msg.Op.Site, msg.Op.Seq) {
			return // 重复投递，忽略
		}
		if !p.doc.Apply(msg.Op) {
			p.sendSyncReqLocked(m.From) // 序号缺口，主动补齐
		}
	case "sync-req":
		var req syncReq
		if json.Unmarshal(m.Data, &req) != nil {
			return
		}
		p.serveSyncLocked(m.From, req.Vector)
		if req.WantAck {
			p.sendSyncReqLocked(m.From)
		}
	case "sync":
		var msg syncMsg
		if json.Unmarshal(m.Data, &msg) != nil {
			return
		}
		for _, op := range msg.Ops {
			p.doc.Apply(op)
		}
	case "snapshot":
		var msg snapshotMsg
		if json.Unmarshal(m.Data, &msg) != nil {
			return
		}
		p.adoptSnapshotLocked(msg.Snap)
	case "presence":
		var msg presenceMsg
		if json.Unmarshal(m.Data, &msg) != nil {
			return
		}
		p.pres.Update(m.From, msg.State)
	case "bye":
		p.pres.Remove(m.From)
	}
}

// serveSyncLocked 响应对方的 sync-req：历史齐全发增量，否则发快照。
func (p *Peer) serveSyncLocked(to string, theirVersion crdt.VersionVector) {
	ops, ok := p.doc.OpsSince(theirVersion)
	if !ok {
		data, _ := json.Marshal(snapshotMsg{Snap: p.doc.Snapshot()})
		p.conn.Send(to, transport.Message{Kind: "snapshot", Data: data})
		return
	}
	data, _ := json.Marshal(syncMsg{Ops: ops})
	p.conn.Send(to, transport.Message{Kind: "sync", Data: data})
}

func (p *Peer) sendSyncReqLocked(to string) {
	data, _ := json.Marshal(syncReq{Vector: p.doc.Version()})
	p.conn.Send(to, transport.Message{Kind: "sync-req", Data: data})
}

// adoptSnapshotLocked 用快照替换本地文档，并重放快照未覆盖的本地操作。
func (p *Peer) adoptSnapshotLocked(snap crdt.Snapshot) {
	mySite := crdt.SiteID(p.id)
	nd := crdt.FromSnapshot(mySite, snap)
	for _, op := range p.localLog {
		if op.Seq > snap.Version[mySite] {
			nd.Apply(op)
		}
	}
	p.doc = nd
}

// --- 后台心跳与周期重同步 ---

func (p *Peer) loop() {
	var hb, rs <-chan time.Time
	var hbTick, rsTick *time.Ticker
	if p.cfg.HeartbeatInterval > 0 {
		hbTick = time.NewTicker(p.cfg.HeartbeatInterval)
		hb = hbTick.C
		defer hbTick.Stop()
	}
	if p.cfg.ResyncInterval > 0 {
		rsTick = time.NewTicker(p.cfg.ResyncInterval)
		rs = rsTick.C
		defer rsTick.Stop()
	}
	for {
		select {
		case <-p.stop:
			return
		case <-hb:
			p.mu.Lock()
			if !p.closed && p.conn != nil {
				data, _ := json.Marshal(presenceMsg{State: p.self})
				p.conn.Broadcast(transport.Message{Kind: "presence", Data: data})
			}
			p.pres.Expire()
			p.mu.Unlock()
		case <-rs:
			p.announce()
		}
	}
}

// --- 本地编辑 ---

func (p *Peer) Insert(pos int, ch rune) {
	p.mu.Lock()
	defer p.mu.Unlock()
	op := p.doc.LocalInsert(pos, ch)
	p.localLog = append(p.localLog, op)
	p.broadcastOpLocked(op)
}

func (p *Peer) Delete(pos int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	op := p.doc.LocalDelete(pos)
	p.localLog = append(p.localLog, op)
	p.broadcastOpLocked(op)
}

// SetAttr 对可见区间 [start, end) 设置属性；value 为空串表示取消。
func (p *Peer) SetAttr(start, end int, key, value string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	op := p.doc.LocalAttr(start, end, key, value)
	p.localLog = append(p.localLog, op)
	p.broadcastOpLocked(op)
}

func (p *Peer) broadcastOpLocked(op crdt.Op) {
	if p.conn == nil {
		return
	}
	data, _ := json.Marshal(opMsg{Op: op})
	p.conn.Broadcast(transport.Message{Kind: "op", Data: data})
}

// UpdatePresence 更新本地光标/选区并立即广播（临时状态，不进历史）。
func (p *Peer) UpdatePresence(s presence.State) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.self = s
	if p.conn != nil {
		data, _ := json.Marshal(presenceMsg{State: s})
		p.conn.Broadcast(transport.Message{Kind: "presence", Data: data})
	}
}

// --- 读取 ---

func (p *Peer) Text() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.doc.Text()
}

func (p *Peer) AttrsAt(pos int) map[string]string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.doc.AttrsAt(pos)
}

func (p *Peer) Version() crdt.VersionVector {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.doc.Version()
}

// Presence 返回当前感知的其他参与者在线状态。
func (p *Peer) Presence() map[string]presence.State { return p.pres.List() }

// Doc 返回底层文档（只读用途；文档自身并发安全）。
func (p *Peer) Doc() *crdt.Document {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.doc
}

// --- 连接控制（测试/模拟用） ---

func (p *Peer) Disconnect() {
	p.mu.Lock()
	conn := p.conn
	p.mu.Unlock()
	if conn != nil {
		conn.Disconnect()
	}
}

func (p *Peer) Reconnect() {
	p.mu.Lock()
	conn := p.conn
	p.mu.Unlock()
	if conn != nil {
		conn.Reconnect()
	}
}

// --- 快照持久化 ---

// Snapshot 序列化当前文档状态（不含任何在线状态）。
func (p *Peer) Snapshot() ([]byte, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return crdt.EncodeSnapshot(p.doc.Snapshot())
}

// Restore 从快照数据恢复（模拟重启），本地未覆盖操作会被重放。
func (p *Peer) Restore(data []byte) error {
	snap, err := crdt.DecodeSnapshot(data)
	if err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.adoptSnapshotLocked(snap)
	return nil
}

// Compact 压缩本地历史；之后过旧的 sync-req 将收到快照响应。
func (p *Peer) Compact() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.doc.Compact()
}
