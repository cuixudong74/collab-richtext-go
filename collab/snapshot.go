package collab

// Snapshot 是文档在某一时刻的可持久化状态。
//
// 快照包含到版本向量为止的全部字符与属性标记。历史压缩会移除
// 不再被任何节点起点或属性锚点引用的墓碑字符，压缩后的快照与
// 仍持有完整历史的一端继续收敛。
type Snapshot struct {
	Version int
	VV      VersionVector
	Chars   []*CharData
	Marks   []*MarkData
}

// CharData 是字符节点的可序列化表示。
type CharData struct {
	ID      OpID
	Origin  OpID
	Value   rune
	Deleted bool
}

// MarkData 是属性标记的可序列化表示。
type MarkData struct {
	ID    OpID
	Attr  string
	Value bool
	Start Anchor
	End   Anchor
}
