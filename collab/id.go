package collab

// ClientID 是参与者（客户端）的唯一标识。
type ClientID uint64

// Clock 是每个参与者本地单调递增的逻辑时钟。
type Clock uint64

// OpID 全局唯一地标识一次操作：(参与者, 该参与者内的时钟)。
type OpID struct {
	Client ClientID
	Clock  Clock
}

// IsZero 判断是否为零值（用于表示文档边界等哨兵位置）。
func (id OpID) IsZero() bool { return id.Client == 0 && id.Clock == 0 }

// Less 提供 OpID 之间的全序比较，用于 RGA 同起点并发插入的胜负判定。
func (id OpID) Less(o OpID) bool {
	if id.Client != o.Client {
		return id.Client < o.Client
	}
	return id.Clock < o.Clock
}

// VersionVector 记录每个参与者已观察到的最大时钟，用于判断操作是否重复、
// 以及断线重连后按区间补齐缺失历史。
type VersionVector map[ClientID]Clock

// NewVV 创建空版本向量。
func NewVV() VersionVector { return VersionVector{} }

// Seen 判断某操作是否已经被观察过（重复投递判定）。
func (vv VersionVector) Seen(id OpID) bool {
	return vv[id.Client] >= id.Clock
}

// MaxClock 返回某参与者已观察到的最大时钟。
func (vv VersionVector) MaxClock(c ClientID) Clock { return vv[c] }

// Observe 记录观察到一个操作，返回是否为新观察到的（时钟推进）。
func (vv VersionVector) Observe(id OpID) bool {
	if vv[id.Client] >= id.Clock {
		return false
	}
	vv[id.Client] = id.Clock
	return true
}

// Merge 合并另一个版本向量（取各参与者时钟的较大值）。
func (vv VersionVector) Merge(other VersionVector) {
	for c, k := range other {
		if k > vv[c] {
			vv[c] = k
		}
	}
}

// Clone 返回一份副本。
func (vv VersionVector) Clone() VersionVector {
	out := make(VersionVector, len(vv))
	for c, k := range vv {
		out[c] = k
	}
	return out
}
