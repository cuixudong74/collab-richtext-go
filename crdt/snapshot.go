package crdt

import "encoding/json"

// Snapshot 是文档在某版本的可持久化状态。
// 快照不包含任何在线状态（光标/选区）。
type Snapshot struct {
	Version VersionVector `json:"version"`
	Base    VersionVector `json:"base"`
	Nodes   []SnapNode    `json:"nodes"`
	AttrOps []Op          `json:"attr_ops,omitempty"`
}

// SnapNode 是节点的可序列化形式，含属性 LWW 仲裁元数据，
// 保证恢复快照后仍能与持有完整历史的一端正确合并属性。
type SnapNode struct {
	ID      ID                `json:"id"`
	Ch      rune              `json:"ch"`
	Deleted bool              `json:"deleted"`
	Attrs   map[string]string `json:"attrs,omitempty"`
	Lams    map[string]uint64 `json:"lams,omitempty"`
	Sites   map[string]SiteID `json:"sites,omitempty"`
}

// Snapshot 生成当前状态的一致性快照。
// 持有读锁，并发读写不会观察到半截状态。
func (d *Document) Snapshot() Snapshot {
	d.mu.RLock()
	defer d.mu.RUnlock()
	snap := Snapshot{
		Version: d.vv.Clone(),
		Base:    d.base.Clone(),
		Nodes:   make([]SnapNode, 0, len(d.nodes)),
		AttrOps: append([]Op(nil), d.attrOps...),
	}
	for _, n := range d.nodes {
		sn := SnapNode{ID: n.id, Ch: n.ch, Deleted: n.deleted}
		if len(n.attrs) > 0 {
			sn.Attrs = make(map[string]string, len(n.attrs))
			sn.Lams = make(map[string]uint64, len(n.attrs))
			sn.Sites = make(map[string]SiteID, len(n.attrs))
			for k, v := range n.attrs {
				sn.Attrs[k] = v.value
				sn.Lams[k] = v.lam
				sn.Sites[k] = v.site
			}
		}
		snap.Nodes = append(snap.Nodes, sn)
	}
	return snap
}

// FromSnapshot 从快照恢复文档。恢复后版本向量与快照一致，
// 后续增量操作可继续收敛。
func FromSnapshot(site SiteID, snap Snapshot) *Document {
	d := NewDocument(site)
	d.vv = snap.Version.Clone()
	d.base = snap.Base.Clone()
	d.attrOps = append([]Op(nil), snap.AttrOps...)
	for _, sn := range snap.Nodes {
		n := &node{id: sn.ID, ch: sn.Ch, deleted: sn.Deleted}
		if len(sn.Attrs) > 0 {
			n.attrs = make(map[string]attrVal, len(sn.Attrs))
			for k, v := range sn.Attrs {
				n.attrs[k] = attrVal{value: v, lam: sn.Lams[k], site: sn.Sites[k]}
			}
		}
		d.index[n.id] = len(d.nodes)
		d.nodes = append(d.nodes, n)
	}
	return d
}

// EncodeSnapshot 将快照序列化为 JSON。
func EncodeSnapshot(s Snapshot) ([]byte, error) {
	return json.Marshal(s)
}

// DecodeSnapshot 从 JSON 还原快照。
func DecodeSnapshot(data []byte) (Snapshot, error) {
	var s Snapshot
	err := json.Unmarshal(data, &s)
	return s, err
}
