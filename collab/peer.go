package collab

import (
	"sync"
	"time"
)

// Peer 是一个协同参与者：持有文档、在线状态，并通过传输层与其他参与者通信。
type Peer struct {
	doc       *Document
	presence  *Presence
	transport Transport
	inbox     <-chan *Message
	cfg       TransportConfig

	mu      sync.Mutex
	peers   map[ClientID]*Presence // 其他参与者的在线状态
	closed  bool
	closeCh chan struct{}
	wg      sync.WaitGroup

	// opLog 记录本端已应用的全部操作（按应用顺序），用于断线重连后按区间补齐缺失历史。
	opLog []*Op

	// 重试：已发送但未确认的操作
	pendingAck map[uint64]*Op
	retries    map[uint64]int
	msgID      uint64
}

// NewPeer 创建一个参与者。
func NewPeer(me ClientID, name string, transport Transport, cfg ...TransportConfig) *Peer {
	var tc TransportConfig
	if len(cfg) > 0 {
		tc = cfg[0]
	} else {
		tc = DefaultTransportConfig()
	}
	p := &Peer{
		doc:        NewDocument(me),
		presence:   &Presence{ClientID: me, Name: name, Online: true, LastSeen: time.Now()},
		transport:  transport,
		cfg:        tc,
		peers:      make(map[ClientID]*Presence),
		closeCh:    make(chan struct{}),
		pendingAck: make(map[uint64]*Op),
		retries:    make(map[uint64]int),
	}
	if lt, ok := transport.(*LoopbackTransport); ok {
		p.inbox = lt.Register(me)
	}
	return p
}

// Doc 返回参与者的文档。
func (p *Peer) Doc() *Document { return p.doc }

// Presence 返回参与者的在线状态。
func (p *Peer) Presence() *Presence { return p.presence }

// Peers 返回其他参与者的在线状态副本。
func (p *Peer) Peers() map[ClientID]*Presence {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make(map[ClientID]*Presence, len(p.peers))
	for k, v := range p.peers {
		cp := *v
		out[k] = &cp
	}
	return out
}

// peerIDs 在锁内复制当前参与者 ID 列表。
func (p *Peer) peerIDs() []ClientID {
	p.mu.Lock()
	defer p.mu.Unlock()
	ids := make([]ClientID, 0, len(p.peers))
	for id := range p.peers {
		ids = append(ids, id)
	}
	return ids
}

// Start 启动参与者的消息处理循环与重试循环。
func (p *Peer) Start() {
	p.wg.Add(2)
	go p.loop()
	go p.retryLoop()
}

// retryLoop 定期检查未确认的操作并重试，直到收到确认或达到最大重试次数。
func (p *Peer) retryLoop() {
	defer p.wg.Done()
	if p.cfg.AckTimeout <= 0 {
		return
	}
	ticker := time.NewTicker(p.cfg.AckTimeout)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			p.retryPending()
		case <-p.closeCh:
			return
		}
	}
}

// retryPending 重发所有未确认且未超过最大重试次数的操作。
func (p *Peer) retryPending() {
	p.mu.Lock()
	var toRetry []struct {
		msgID uint64
		op    *Op
	}
	for msgID, op := range p.pendingAck {
		if p.retries[msgID] < p.cfg.MaxRetries {
			toRetry = append(toRetry, struct {
				msgID uint64
				op    *Op
			}{msgID, op})
			p.retries[msgID]++
		}
	}
	p.mu.Unlock()
	for _, item := range toRetry {
		msg := &Message{From: p.doc.Me(), Kind: MsgOp, MsgID: item.msgID, Op: item.op}
		for _, peer := range p.peerIDs() {
			m := *msg
			m.To = peer
			p.transport.Send(&m)
		}
	}
}

// loop 是消息处理主循环。
func (p *Peer) loop() {
	defer p.wg.Done()
	for {
		select {
		case msg, ok := <-p.inbox:
			if !ok {
				return
			}
			p.handleMessage(msg)
		case <-p.closeCh:
			return
		}
	}
}

// handleMessage 处理一条收到的消息。
func (p *Peer) handleMessage(msg *Message) {
	switch msg.Kind {
	case MsgOp:
		if msg.Op != nil {
			p.applyAndLog(msg.Op)
			p.sendAck(msg)
		}
	case MsgAck:
		p.mu.Lock()
		delete(p.pendingAck, msg.MsgID)
		p.mu.Unlock()
	case MsgPresence:
		p.mu.Lock()
		if msg.Pres != nil {
			cp := *msg.Pres
			cp.LastSeen = time.Now()
			p.peers[msg.From] = &cp
		}
		p.mu.Unlock()
	case MsgJoin:
		p.mu.Lock()
		if msg.Pres != nil {
			cp := *msg.Pres
			cp.LastSeen = time.Now()
			p.peers[msg.From] = &cp
		}
		p.mu.Unlock()
		p.broadcastPresence()
		p.sendSyncRequest(msg.From)
	case MsgLeave:
		p.mu.Lock()
		delete(p.peers, msg.From)
		p.mu.Unlock()
	case MsgSyncRequest:
		p.handleSyncRequest(msg)
	case MsgSyncData:
		p.handleSyncData(msg)
	}
}

// applyAndLog 应用操作并追加到操作日志。
func (p *Peer) applyAndLog(op *Op) {
	if err := p.doc.Apply(op); err == nil {
		p.mu.Lock()
		p.opLog = append(p.opLog, op)
		p.mu.Unlock()
	}
}

// sendAck 发送确认。
func (p *Peer) sendAck(orig *Message) {
	p.transport.Send(&Message{
		From: p.doc.Me(), To: orig.From, Kind: MsgAck, MsgID: orig.MsgID,
	})
}

// nextMsgID 分配下一个消息 ID。
func (p *Peer) nextMsgID() uint64 {
	p.msgID++
	return p.msgID
}

// broadcastPresence 广播自己的在线状态。
func (p *Peer) broadcastPresence() {
	p.mu.Lock()
	pres := *p.presence
	p.mu.Unlock()
	msg := &Message{From: p.doc.Me(), Kind: MsgPresence, Pres: &pres}
	for _, peer := range p.peerIDs() {
		m := *msg
		m.To = peer
		p.transport.Send(&m)
	}
}

// sendOp 发送一个本地操作给所有其他参与者。
func (p *Peer) sendOp(op *Op) {
	p.mu.Lock()
	msgID := p.nextMsgID()
	p.pendingAck[msgID] = op
	p.mu.Unlock()
	msg := &Message{From: p.doc.Me(), Kind: MsgOp, MsgID: msgID, Op: op}
	for _, peer := range p.peerIDs() {
		m := *msg
		m.To = peer
		p.transport.Send(&m)
	}
}

// Insert 本地插入文本并广播操作。
func (p *Peer) Insert(pos int, text string) []*Op {
	ops := p.doc.Insert(pos, text)
	for _, op := range ops {
		p.applyAndLog(op)
		p.sendOp(op)
	}
	return ops
}

// Delete 本地删除并广播操作。
func (p *Peer) Delete(start, end int) []*Op {
	ops := p.doc.Delete(start, end)
	for _, op := range ops {
		p.applyAndLog(op)
		p.sendOp(op)
	}
	return ops
}

// SetAttr 本地设置属性并广播操作。
func (p *Peer) SetAttr(start, end int, attr string, value bool) *Op {
	op := p.doc.SetAttr(start, end, attr, value)
	if op != nil {
		p.applyAndLog(op)
		p.sendOp(op)
	}
	return op
}

// UpdateCursor 更新光标位置并广播。
func (p *Peer) UpdateCursor(cursor Anchor) {
	p.mu.Lock()
	p.presence.Cursor = cursor
	pres := *p.presence
	p.mu.Unlock()
	msg := &Message{From: p.doc.Me(), Kind: MsgPresence, Pres: &pres}
	for _, peer := range p.peerIDs() {
		m := *msg
		m.To = peer
		p.transport.Send(&m)
	}
}

// UpdateSelection 更新选区并广播。
func (p *Peer) UpdateSelection(start, end Anchor) {
	p.mu.Lock()
	p.presence.SelStart = start
	p.presence.SelEnd = end
	pres := *p.presence
	p.mu.Unlock()
	msg := &Message{From: p.doc.Me(), Kind: MsgPresence, Pres: &pres}
	for _, peer := range p.peerIDs() {
		m := *msg
		m.To = peer
		p.transport.Send(&m)
	}
}

// Join 加入协同会话，广播加入消息。
func (p *Peer) Join() {
	p.mu.Lock()
	p.presence.Online = true
	p.presence.LastSeen = time.Now()
	pres := *p.presence
	p.mu.Unlock()
	msg := &Message{From: p.doc.Me(), Kind: MsgJoin, Pres: &pres}
	for _, peer := range p.peerIDs() {
		m := *msg
		m.To = peer
		p.transport.Send(&m)
	}
}

// Leave 离开协同会话，广播离开消息。
func (p *Peer) Leave() {
	p.mu.Lock()
	p.presence.Online = false
	pres := *p.presence
	p.mu.Unlock()
	msg := &Message{From: p.doc.Me(), Kind: MsgLeave, Pres: &pres}
	for _, peer := range p.peerIDs() {
		m := *msg
		m.To = peer
		p.transport.Send(&m)
	}
}

// sendSyncRequest 向对端发送同步请求（携带自己的版本向量）。
func (p *Peer) sendSyncRequest(to ClientID) {
	vv := p.doc.VV()
	p.transport.Send(&Message{
		From: p.doc.Me(), To: to, Kind: MsgSyncRequest, VV: vv,
	})
}

// handleSyncRequest 处理同步请求：补齐对端缺失的操作。
// 仅当本端也可能缺失操作时才回发同步请求，避免无限循环。
func (p *Peer) handleSyncRequest(msg *Message) {
	p.mu.Lock()
	// 找出对端缺失的操作：本端 opLog 中时钟 > 对端 vv 对应时钟的操作
	var missing []*Op
	for _, op := range p.opLog {
		if op.ID.Clock > msg.VV[op.ID.Client] {
			missing = append(missing, op)
		}
	}
	// 判断本端是否可能缺失对端的操作：对端 vv 中是否有本端未知的客户端时钟
	needSync := false
	for c, k := range msg.VV {
		if k > p.doc.VV()[c] {
			needSync = true
		}
	}
	p.mu.Unlock()

	// 发送缺失操作
	if len(missing) > 0 {
		p.transport.Send(&Message{
			From: p.doc.Me(), To: msg.From, Kind: MsgSyncData, Ops: missing,
		})
	}

	// 仅当本端可能缺失操作时才回发同步请求
	if needSync {
		p.sendSyncRequest(msg.From)
	}
}

// handleSyncData 处理同步数据：应用对端发来的缺失操作。
func (p *Peer) handleSyncData(msg *Message) {
	for _, op := range msg.Ops {
		p.applyAndLog(op)
	}
}

// Close 关闭参与者。
func (p *Peer) Close() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	close(p.closeCh)
	p.mu.Unlock()
	p.wg.Wait()
}

// CleanupStalePeers 清理超过 timeout 未活动的参与者（异常掉线自动清理）。
// 返回被清理的参与者 ID 列表。
func (p *Peer) CleanupStalePeers(timeout time.Duration) []ClientID {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := time.Now()
	var removed []ClientID
	for id, pres := range p.peers {
		if now.Sub(pres.LastSeen) > timeout {
			delete(p.peers, id)
			removed = append(removed, id)
		}
	}
	return removed
}
