package crdt

// OpKind 操作类型。
type OpKind int

const (
	OpInsert OpKind = iota // 插入字符
	OpDelete               // 删除字符（墓碑）
	OpAttr                 // 区间属性设置/取消
)

// Op 是一次文档变更。每个 Op 由 (Site, Seq) 唯一标识，
// 重复投递同一 Op 不会产生二次影响。
type Op struct {
	Site SiteID
	Seq  uint64 // 该 Site 的递增序号
	Lam  uint64 // Lamport 时钟，用于属性 LWW 仲裁
	Kind OpKind

	// Insert 字段
	Parent ID   // 左邻节点 ID（文档头为 RootID）
	Node   ID   // 新节点 ID
	Ch     rune // 插入的字符

	// Delete 字段
	Target ID // 被删除节点 ID

	// Attr 字段：作用于 ID 区间 (Start, End) 内的所有节点
	Start ID
	End   ID
	Key   string // 属性名，如 "bold"
	Value string // 属性值；空串表示取消该属性
}

// RootID 是文档虚拟头节点的 ID。
var RootID = ID{Seq: 0, Site: ""}
