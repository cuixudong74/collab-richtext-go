package collab

// OpType 标识操作类型。
type OpType uint8

const (
	OpInsert OpType = iota
	OpDelete
	OpMark
)

// AnchorAssoc 描述锚点相对字符的偏向：紧贴字符左侧或右侧。
type AnchorAssoc uint8

const (
	AssocBefore AnchorAssoc = iota // 字符左侧
	AssocAfter                     // 字符右侧
)

// Anchor 是文档中的一个稳定位置引用。
//
// 当 Char 为零值时表示文档边界：
//   - Char 零值 + AssocAfter  => 文档起点
//   - Char 零值 + AssocBefore => 文档终点
//
// 当 Char 指向某个已插入字符时：
//   - AssocBefore => 紧贴该字符左侧
//   - AssocAfter  => 紧贴该字符右侧
type Anchor struct {
	Char  OpID
	Assoc AnchorAssoc
}

// DocStart 返回文档起点锚点。
func DocStart() Anchor { return Anchor{Char: OpID{}, Assoc: AssocAfter} }

// DocEnd 返回文档终点锚点。
func DocEnd() Anchor { return Anchor{Char: OpID{}, Assoc: AssocBefore} }

// Op 是一次文档变更。所有字段对所有操作类型复用，便于序列化与重放。
type Op struct {
	ID   OpID
	Type OpType

	// OpInsert: Origin 是插入位置左侧的字符（零值表示起点），Value 是字符内容。
	Origin OpID
	Value  rune

	// OpDelete: Target 是被删除字符的 ID。
	Target OpID

	// OpMark: Attr/Value 是属性名与取值，Start/End 是属性覆盖的区间（前闭后开）。
	Attr  string
	MVal  bool
	Start Anchor
	End   Anchor
}

// InsertOp 构造一个插入操作。
func InsertOp(id OpID, origin OpID, value rune) *Op {
	return &Op{ID: id, Type: OpInsert, Origin: origin, Value: value}
}

// DeleteOp 构造一个删除操作。
func DeleteOp(id OpID, target OpID) *Op {
	return &Op{ID: id, Type: OpDelete, Target: target}
}

// MarkOp 构造一个属性操作。
func MarkOp(id OpID, attr string, value bool, start, end Anchor) *Op {
	return &Op{ID: id, Type: OpMark, Attr: attr, MVal: value, Start: start, End: end}
}
