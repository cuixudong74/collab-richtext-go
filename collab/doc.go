package collab

import (
	"sort"
	"sync"
)

// CharNode 是 RGA 链表中的一个节点（含墓碑）。
type CharNode struct {
	id      OpID
	origin  OpID
	value   rune
	deleted bool
	next    *CharNode
}

// Mark 是一次属性设置操作（Peritext 风格的区间标记）。
type Mark struct {
	id    OpID
	attr  string
	value bool
	start Anchor
	end   Anchor
}

// Document 是富文本协同内核的状态容器。
//
// 并发模型：Document 用一把读写锁保护全部状态。本地读取（Text/RichText）
// 与远端操作应用、快照写入同时发生时，读取方通过 RLock 获得一致性快照，
// 不会读到半截状态，也不存在数据竞争。
type Document struct {
	mu sync.RWMutex

	me    ClientID
	clock Clock
	vv    VersionVector

	head  *CharNode
	nodes map[OpID]*CharNode
	marks map[OpID]*Mark

	// deleted 记录已被删除的字符 ID。删除可能先于插入到达，
	// 因此插入时也要检查该集合，保证删除与插入满足交换律。
	deleted map[OpID]bool

	// pending 是因依赖未满足而暂存的操作（如插入的起点字符尚未到达、
	// 属性锚点字符尚未到达）。每次应用新操作后尝试刷新。
	pending []*Op
}

// NewDocument 创建一个属于参与者 me 的空文档。
func NewDocument(me ClientID) *Document {
	d := &Document{
		me:      me,
		vv:      NewVV(),
		nodes:   make(map[OpID]*CharNode),
		marks:   make(map[OpID]*Mark),
		deleted: make(map[OpID]bool),
	}
	d.head = &CharNode{} // 哨兵，id 为零值
	return d
}

// Me 返回文档所属参与者 ID。
func (d *Document) Me() ClientID { return d.me }

// VV 返回版本向量副本。
func (d *Document) VV() VersionVector {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.vv.Clone()
}

// nextID 分配下一个本地操作 ID（调用方持锁）。
func (d *Document) nextID() OpID {
	d.clock++
	return OpID{Client: d.me, Clock: d.clock}
}

// ---------------------------------------------------------------------------
// 操作应用
// ---------------------------------------------------------------------------

// Apply 应用一个操作。重复投递（已在版本向量中）直接返回 nil，不产生二次影响。
// 乱序到达（依赖未满足）的操作会被暂存，待依赖到达后自动刷新。
func (d *Document) Apply(op *Op) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.vv.Seen(op.ID) {
		return nil // 幂等：重复投递
	}
	if !d.canApply(op) {
		d.pending = append(d.pending, op)
		return nil
	}
	d.applyNow(op)
	d.flushPending()
	return nil
}

// canApply 判断操作的依赖是否已满足（调用方持锁）。
//
// 除了操作自身的结构依赖（插入起点、属性锚点）外，还要求同一参与者的
// 操作按因果顺序应用：op.ID.Clock 必须恰好等于 vv[client]+1。否则高时钟
// 操作先到达时会跳过低时钟操作，而版本向量用最大时钟判重会把缺失的低时钟
// 操作误判为"已见"，导致其永不应用。
func (d *Document) canApply(op *Op) bool {
	if op.ID.Clock > d.vv[op.ID.Client]+1 {
		return false
	}
	switch op.Type {
	case OpInsert:
		if op.Origin.IsZero() {
			return true
		}
		_, ok := d.nodes[op.Origin]
		return ok
	case OpMark:
		return d.anchorReady(op.Start) && d.anchorReady(op.End)
	}
	return true
}

func (d *Document) anchorReady(a Anchor) bool {
	if a.Char.IsZero() {
		return true
	}
	_, ok := d.nodes[a.Char]
	return ok
}

// applyNow 立即应用一个已满足依赖的操作（调用方持锁）。
func (d *Document) applyNow(op *Op) {
	d.vv.Observe(op.ID)
	switch op.Type {
	case OpInsert:
		n := &CharNode{id: op.ID, origin: op.Origin, value: op.Value}
		if d.deleted[op.ID] {
			n.deleted = true // 删除先于插入到达
		}
		d.insertRGA(n)
		d.nodes[op.ID] = n
	case OpDelete:
		d.deleted[op.Target] = true
		if n, ok := d.nodes[op.Target]; ok {
			n.deleted = true
		}
	case OpMark:
		if _, exists := d.marks[op.ID]; !exists {
			d.marks[op.ID] = &Mark{
				id:    op.ID,
				attr:  op.Attr,
				value: op.MVal,
				start: op.Start,
				end:   op.End,
			}
		}
	}
}

// flushPending 反复尝试应用暂存操作，直到没有进展（调用方持锁）。
func (d *Document) flushPending() {
	for {
		progress := false
		rest := d.pending[:0]
		for _, op := range d.pending {
			if d.vv.Seen(op.ID) {
				progress = true
				continue
			}
			if d.canApply(op) {
				d.applyNow(op)
				progress = true
			} else {
				rest = append(rest, op)
			}
		}
		d.pending = rest
		if !progress {
			return
		}
	}
}

// insertRGA 按 RGA 规则把节点插入链表（调用方持锁）。
//
// 规则：从起点节点 origin 出发向右扫描，
//   - 若下一节点与新节点同起点（origin 相同），ID 更大的排在前面（降序）；
//   - 若下一节点起点不同，仅当其起点排在 origin 之前时才越过它。
//
// 该规则保证同一组操作以任意顺序应用都收敛到同一链表。
func (d *Document) insertRGA(n *CharNode) {
	origin := d.head
	if !n.origin.IsZero() {
		origin = d.nodes[n.origin]
	}
	prev := origin
	for prev.next != nil {
		next := prev.next
		if next.origin == n.origin {
			if next.id.Less(n.id) {
				break
			}
			prev = next
		} else {
			if next.origin.Less(n.origin) {
				break
			}
			prev = next
		}
	}
	n.next = prev.next
	prev.next = n
}

// ---------------------------------------------------------------------------
// 本地编辑
// ---------------------------------------------------------------------------

// Insert 在实时位置 pos（以 0 为基的存活字符偏移）插入文本，返回产生的操作。
func (d *Document) Insert(pos int, text string) []*Op {
	d.mu.Lock()
	defer d.mu.Unlock()

	live := d.liveNodes()
	var origin OpID
	if pos <= 0 {
		origin = OpID{}
	} else if pos > len(live) {
		origin = live[len(live)-1].id
	} else {
		origin = live[pos-1].id
	}
	ops := make([]*Op, 0, len(text))
	for _, r := range text {
		id := d.nextID()
		op := InsertOp(id, origin, r)
		d.applyNow(op)
		origin = id
		ops = append(ops, op)
	}
	return ops
}

// Delete 删除实时区间 [start, end)，返回产生的操作。
func (d *Document) Delete(start, end int) []*Op {
	d.mu.Lock()
	defer d.mu.Unlock()

	live := d.liveNodes()
	if start < 0 {
		start = 0
	}
	if end > len(live) {
		end = len(live)
	}
	if start >= end {
		return nil
	}
	ops := make([]*Op, 0, end-start)
	for i := start; i < end; i++ {
		id := d.nextID()
		op := DeleteOp(id, live[i].id)
		d.applyNow(op)
		ops = append(ops, op)
	}
	return ops
}

// SetAttr 对实时区间 [start, end) 设置属性，返回产生的操作。
func (d *Document) SetAttr(start, end int, attr string, value bool) *Op {
	d.mu.Lock()
	defer d.mu.Unlock()

	live := d.liveNodes()
	if start < 0 {
		start = 0
	}
	if end > len(live) {
		end = len(live)
	}
	if start >= end {
		return nil
	}
	id := d.nextID()
	var s, e Anchor
	if start == 0 {
		s = DocStart()
	} else {
		s = Anchor{Char: live[start].id, Assoc: AssocBefore}
	}
	// 终点锚定到区间末尾字符的右侧，而非 DocEnd()。DocEnd 是随末尾插入
	// 移动的锚点，会导致后续在末尾追加的文本被错误纳入属性区间。
	if len(live) > 0 {
		e = Anchor{Char: live[end-1].id, Assoc: AssocAfter}
	} else {
		e = DocEnd()
	}
	op := MarkOp(id, attr, value, s, e)
	d.applyNow(op)
	return op
}

// ---------------------------------------------------------------------------
// 渲染
// ---------------------------------------------------------------------------

// liveNodes 返回按文档顺序排列的存活字符节点（调用方持锁）。
func (d *Document) liveNodes() []*CharNode {
	var out []*CharNode
	for cur := d.head.next; cur != nil; cur = cur.next {
		if !cur.deleted {
			out = append(out, cur)
		}
	}
	return out
}

// Text 返回当前存活正文。
func (d *Document) Text() string {
	d.mu.RLock()
	defer d.mu.RUnlock()
	var b []rune
	for cur := d.head.next; cur != nil; cur = cur.next {
		if !cur.deleted {
			b = append(b, cur.value)
		}
	}
	return string(b)
}

// Span 是一段带属性的连续文本。
type Span struct {
	Text  string
	Attrs map[string]bool
}

// RichText 返回带属性的正文：每个存活字符位置上，每个属性取覆盖它的
// 属性标记中 ID 最大者（LWW），未被任何标记覆盖时取属性默认值 false。
func (d *Document) RichText() []Span {
	d.mu.RLock()
	defer d.mu.RUnlock()

	live := d.liveNodes()
	n := len(live)
	// 收集涉及的属性名
	attrSet := make(map[string]bool)
	for _, m := range d.marks {
		attrSet[m.attr] = true
	}
	attrs := make([]string, 0, len(attrSet))
	for a := range attrSet {
		attrs = append(attrs, a)
	}
	sort.Strings(attrs)

	// 每个位置每个属性的最优标记 ID 与取值
	bestID := make(map[string]OpID, n*len(attrs))
	bestVal := make(map[string]bool, n*len(attrs))
	for _, m := range d.marks {
		si := d.anchorIndex(m.start, live)
		ei := d.anchorIndex(m.end, live)
		if si > ei {
			si, ei = ei, si
		}
		for i := si; i < ei; i++ {
			key := attrKey(i, m.attr)
			if cur, ok := bestID[key]; !ok || cur.Less(m.id) {
				bestID[key] = m.id
				bestVal[key] = m.value
			}
		}
	}

	// 组装 span
	var spans []Span
	cur := Span{Attrs: map[string]bool{}}
	flush := func() {
		if len(cur.Text) > 0 {
			spans = append(spans, cur)
		}
	}
	for i, node := range live {
		ch := string(node.value)
		a := map[string]bool{}
		for _, attr := range attrs {
			a[attr] = bestVal[attrKey(i, attr)]
		}
		if len(cur.Text) > 0 && sameAttrs(cur.Attrs, a) {
			cur.Text += ch
		} else {
			flush()
			cur = Span{Text: ch, Attrs: a}
		}
	}
	flush()
	return spans
}

func attrKey(i int, attr string) string {
	return string(rune(i)) + "\x00" + attr
}

func sameAttrs(a, b map[string]bool) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range b {
		if a[k] != v {
			return false
		}
	}
	return true
}

// anchorIndex 计算锚点在存活字符数组中的下标（调用方持锁）。
//
// 锚点字符已删除时，锚点停留在该字符所在位置；锚点字符尚未到达时，
// 起点锚点解析为 0、终点锚点解析为末尾（属性标记会在依赖到达后重算）。
func (d *Document) anchorIndex(a Anchor, live []*CharNode) int {
	if a.Char.IsZero() {
		if a.Assoc == AssocAfter {
			return 0
		}
		return len(live)
	}
	count := 0
	for cur := d.head.next; cur != nil; cur = cur.next {
		if cur.id == a.Char {
			if a.Assoc == AssocBefore {
				return count
			}
			if !cur.deleted {
				count++
			}
			return count
		}
		if !cur.deleted {
			count++
		}
	}
	// 锚点字符尚未到达
	if a.Assoc == AssocAfter {
		return 0
	}
	return len(live)
}

// ---------------------------------------------------------------------------
// 快照与历史压缩
// ---------------------------------------------------------------------------

// Snapshot 生成当前文档的可持久化快照。
func (d *Document) Snapshot() *Snapshot {
	d.mu.RLock()
	defer d.mu.RUnlock()

	snap := &Snapshot{Version: 1, VV: d.vv.Clone()}
	for cur := d.head.next; cur != nil; cur = cur.next {
		snap.Chars = append(snap.Chars, &CharData{
			ID: cur.id, Origin: cur.origin, Value: cur.value, Deleted: cur.deleted,
		})
	}
	for _, m := range d.marks {
		snap.Marks = append(snap.Marks, &MarkData{
			ID: m.id, Attr: m.attr, Value: m.value, Start: m.start, End: m.end,
		})
	}
	return snap
}

// Restore 从快照恢复文档状态（清空当前状态）。
func (d *Document) Restore(snap *Snapshot) {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.head = &CharNode{}
	d.nodes = make(map[OpID]*CharNode)
	d.marks = make(map[OpID]*Mark)
	d.deleted = make(map[OpID]bool)
	d.pending = nil
	d.vv = snap.VV.Clone()

	// 按快照顺序重建链表
	var prev *CharNode = d.head
	for _, c := range snap.Chars {
		n := &CharNode{
			id: c.ID, origin: c.Origin, value: c.Value, deleted: c.Deleted,
		}
		prev.next = n
		prev = n
		d.nodes[c.ID] = n
		if c.Deleted {
			d.deleted[c.ID] = true
		}
	}
	for _, m := range snap.Marks {
		d.marks[m.ID] = &Mark{
			id: m.ID, attr: m.Attr, value: m.Value, start: m.Start, end: m.End,
		}
	}
}

// Compact 历史压缩：移除不再被任何节点起点或属性锚点引用的墓碑字符。
// 压缩后文档与仍持有完整历史的一端继续收敛。
func (d *Document) Compact() int {
	d.mu.Lock()
	defer d.mu.Unlock()

	// 统计引用：哪些字符被节点起点或属性锚点引用
	referenced := make(map[OpID]bool)
	for _, n := range d.nodes {
		if !n.origin.IsZero() {
			referenced[n.origin] = true
		}
	}
	for _, m := range d.marks {
		if !m.start.Char.IsZero() {
			referenced[m.start.Char] = true
		}
		if !m.end.Char.IsZero() {
			referenced[m.end.Char] = true
		}
	}

	removed := 0
	prev := d.head
	for cur := d.head.next; cur != nil; {
		next := cur.next
		if cur.deleted && !referenced[cur.id] {
			// 安全移除：无任何引用
			prev.next = next
			delete(d.nodes, cur.id)
			removed++
		} else {
			prev = cur
		}
		cur = next
	}
	return removed
}

// ---------------------------------------------------------------------------
// 同步
// ---------------------------------------------------------------------------

// (同步逻辑在 Peer 层维护操作日志，按版本向量区间补齐。)
