package crdt

import (
	"math"
	"sync"
)

// EndID 是文档虚拟尾节点的 ID，大于任何真实节点 ID。
var EndID = ID{Seq: math.MaxUint64, Site: "\xff"}

// attrVal 记录某节点某属性的当前值及仲裁时间戳（LWW）。
type attrVal struct {
	value string
	lam   uint64
	site  SiteID
}

// node 是文档序列中的一个字符节点（RGA 元素）。
type node struct {
	id      ID
	ch      rune
	deleted bool
	attrs   map[string]attrVal
}

// Document 是富文本协同文档内核。
// 所有导出方法均并发安全；远端应用、本地读取与快照可安全并发。
type Document struct {
	mu      sync.RWMutex
	site    SiteID
	lam     uint64
	nodes   []*node
	index   map[ID]int
	log     []Op          // 已应用的历史（Compact 后只保留增量）
	attrOps []Op          // 全部属性操作，永久保留以保证插入顺序无关性
	base    VersionVector // log 起点对应的版本（压缩基线）
	vv      VersionVector
	pending []Op // 依赖未满足或序号有空缺的操作
}

func NewDocument(site SiteID) *Document {
	return &Document{
		site:  site,
		index: make(map[ID]int),
		base:  make(VersionVector),
		vv:    make(VersionVector),
	}
}

// --- 本地操作生成 ---

// LocalInsert 在可见位置 pos（[0, Len]）插入字符，返回已应用的操作。
func (d *Document) LocalInsert(pos int, ch rune) Op {
	d.mu.Lock()
	defer d.mu.Unlock()
	parent := RootID
	if pos > 0 {
		parent = d.visibleIDLocked(pos - 1)
	}
	op := Op{
		Site:   d.site,
		Seq:    d.vv[d.site] + 1,
		Lam:    d.lam + 1,
		Kind:   OpInsert,
		Parent: parent,
		Node:   ID{Seq: d.vv[d.site] + 1, Site: d.site},
		Ch:     ch,
	}
	d.applyLocked(op)
	return op
}

// LocalDelete 删除可见位置 pos 处的字符。
func (d *Document) LocalDelete(pos int) Op {
	d.mu.Lock()
	defer d.mu.Unlock()
	target := d.visibleIDLocked(pos)
	op := Op{
		Site:   d.site,
		Seq:    d.vv[d.site] + 1,
		Lam:    d.lam + 1,
		Kind:   OpDelete,
		Target: target,
	}
	d.applyLocked(op)
	return op
}

// LocalAttr 对可见区间 [startPos, endPos) 设置属性；value 为空串表示取消。
func (d *Document) LocalAttr(startPos, endPos int, key, value string) Op {
	d.mu.Lock()
	defer d.mu.Unlock()
	start := RootID
	if startPos > 0 {
		start = d.visibleIDLocked(startPos - 1)
	}
	end := EndID
	if endPos < d.visibleLenLocked() {
		end = d.visibleIDLocked(endPos)
	}
	op := Op{
		Site:  d.site,
		Seq:   d.vv[d.site] + 1,
		Lam:   d.lam + 1,
		Kind:  OpAttr,
		Start: start,
		End:   end,
		Key:   key,
		Value: value,
	}
	d.applyLocked(op)
	return op
}

// --- 操作应用 ---

// Apply 应用一个（通常为远端的）操作。
// 返回 false 表示该操作是重复的或暂被缓存等待依赖。
// 重复操作绝不产生二次影响。
func (d *Document) Apply(op Op) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if !d.tryApply(op) {
		return false
	}
	d.drainPendingLocked()
	return true
}

// tryApply 尝试应用；重复操作视为已处理（返回 true 但不重复应用）。
func (d *Document) tryApply(op Op) bool {
	if d.vv[op.Site] >= op.Seq {
		return true // 重复投递，忽略
	}
	if op.Seq != d.vv[op.Site]+1 {
		d.pending = append(d.pending, op) // 序号空缺，等待补齐
		return false
	}
	if !d.depsReadyLocked(op) {
		d.pending = append(d.pending, op) // 跨端依赖未到，等待
		return false
	}
	d.applyLocked(op)
	return true
}

func (d *Document) depsReadyLocked(op Op) bool {
	switch op.Kind {
	case OpInsert:
		if op.Parent.IsZero() {
			return true // RootID
		}
		_, ok := d.index[op.Parent]
		return ok
	case OpDelete:
		_, ok := d.index[op.Target]
		return ok
	}
	return true
}

// applyLocked 真正执行操作，调用前必须已满足全部前置条件。
func (d *Document) applyLocked(op Op) {
	switch op.Kind {
	case OpInsert:
		d.insertLocked(op)
	case OpDelete:
		if i, ok := d.index[op.Target]; ok {
			d.nodes[i].deleted = true
		}
	case OpAttr:
		d.attrLocked(op)
		d.attrOps = append(d.attrOps, op)
	}
	d.vv[op.Site] = op.Seq
	if op.Lam > d.lam {
		d.lam = op.Lam
	}
	d.log = append(d.log, op)
}

// insertLocked 按 RGA 规则插入：越过所有 ID 更大的并发兄弟节点。
// 新节点落入既有属性区间时，按 LWW 继承对应属性，
// 保证“先设属性后收到插入”与“先收到插入后设属性”结果一致。
func (d *Document) insertLocked(op Op) {
	n := &node{id: op.Node, ch: op.Ch}
	for _, ao := range d.attrOps {
		if n.id.Compare(ao.Start) > 0 && n.id.Compare(ao.End) < 0 {
			d.mergeAttrLocked(n, ao)
		}
	}
	at := 0
	if !op.Parent.IsZero() {
		at = d.index[op.Parent] + 1
	}
	for at < len(d.nodes) && d.nodes[at].id.Compare(op.Node) > 0 {
		at++
	}
	d.nodes = append(d.nodes, nil)
	copy(d.nodes[at+1:], d.nodes[at:])
	d.nodes[at] = n
	for i := at; i < len(d.nodes); i++ {
		d.index[d.nodes[i].id] = i
	}
}

// attrLocked 对 ID 区间 (Start, End) 内的节点做属性 LWW 合并。
// 区间按节点 ID 界定而非位置，因此与并发增删合并后结果确定。
// 墓碑节点同样被更新，但渲染时不可见，不会牵连存活内容。
func (d *Document) attrLocked(op Op) {
	for _, n := range d.nodes {
		if n.id.Compare(op.Start) <= 0 || n.id.Compare(op.End) >= 0 {
			continue
		}
		if n.attrs == nil {
			n.attrs = make(map[string]attrVal)
		}
		d.mergeAttrLocked(n, op)
	}
}

// mergeAttrLocked 按 (Lam, Site) 的 LWW 规则把属性操作合并到单个节点。
func (d *Document) mergeAttrLocked(n *node, op Op) {
	if n.attrs == nil {
		n.attrs = make(map[string]attrVal)
	}
	cur, ok := n.attrs[op.Key]
	if ok && (cur.lam > op.Lam || (cur.lam == op.Lam && cur.site >= op.Site)) {
		return // 已有更新的值
	}
	n.attrs[op.Key] = attrVal{value: op.Value, lam: op.Lam, site: op.Site}
}

// drainPendingLocked 反复尝试应用被缓存的操作，直到没有进展。
func (d *Document) drainPendingLocked() {
	for len(d.pending) > 0 {
		progress := false
		rest := d.pending[:0]
		for _, op := range d.pending {
			if d.vv[op.Site] >= op.Seq {
				progress = true // 重复，丢弃
				continue
			}
			if op.Seq == d.vv[op.Site]+1 && d.depsReadyLocked(op) {
				d.applyLocked(op)
				progress = true
				continue
			}
			rest = append(rest, op)
		}
		d.pending = rest
		if !progress {
			return
		}
	}
}

// --- 读取 ---

// Text 返回当前可见正文。
func (d *Document) Text() string {
	d.mu.RLock()
	defer d.mu.RUnlock()
	var out []rune
	for _, n := range d.nodes {
		if !n.deleted {
			out = append(out, n.ch)
		}
	}
	return string(out)
}

// Len 返回可见字符数。
func (d *Document) Len() int {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.visibleLenLocked()
}

// AttrsAt 返回可见位置 pos 处字符的属性集合。
func (d *Document) AttrsAt(pos int) map[string]string {
	d.mu.RLock()
	defer d.mu.RUnlock()
	n := d.visibleNodeLocked(pos)
	if n == nil {
		return nil
	}
	out := make(map[string]string, len(n.attrs))
	for k, v := range n.attrs {
		if v.value != "" {
			out[k] = v.value
		}
	}
	return out
}

// Version 返回当前版本向量的副本。
func (d *Document) Version() VersionVector {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.vv.Clone()
}

// Base 返回压缩基线版本。
func (d *Document) Base() VersionVector {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.base.Clone()
}

// --- 历史与压缩 ---

// OpsSince 返回使 since 推进到当前版本所需的全部操作。
// 若请求的部分历史已被压缩，返回 ok=false，调用方应改用快照对齐。
func (d *Document) OpsSince(since VersionVector) (ops []Op, ok bool) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	for site, seq := range d.base {
		if since[site] < seq {
			return nil, false // 请求方需要的历史已被压缩
		}
	}
	for _, op := range d.log {
		if op.Seq > since[op.Site] {
			ops = append(ops, op)
		}
	}
	return ops, true
}

// Compact 将当前全部历史压缩进基线，丢弃操作日志。
// 压缩不影响后续与持有完整历史的一端继续收敛。
func (d *Document) Compact() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.base = d.vv.Clone()
	d.log = nil
}

// --- 内部辅助（须持锁） ---

func (d *Document) visibleLenLocked() int {
	n := 0
	for _, nd := range d.nodes {
		if !nd.deleted {
			n++
		}
	}
	return n
}

func (d *Document) visibleNodeLocked(pos int) *node {
	i := 0
	for _, nd := range d.nodes {
		if nd.deleted {
			continue
		}
		if i == pos {
			return nd
		}
		i++
	}
	return nil
}

func (d *Document) visibleIDLocked(pos int) ID {
	if n := d.visibleNodeLocked(pos); n != nil {
		return n.id
	}
	return EndID
}
