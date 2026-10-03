package crdt

// SiteID 唯一标识一个参与者（副本）。
type SiteID string

// ID 是文档中每个字符节点的全局唯一标识。
// 同一 Site 内 Seq 单调递增；跨 Site 通过 (Seq, Site) 全序比较。
type ID struct {
	Seq  uint64
	Site SiteID
}

// Compare 返回 -1/0/1，定义 ID 的全序关系。
func (a ID) Compare(b ID) int {
	if a.Seq != b.Seq {
		if a.Seq < b.Seq {
			return -1
		}
		return 1
	}
	if a.Site < b.Site {
		return -1
	}
	if a.Site > b.Site {
		return 1
	}
	return 0
}

func (a ID) IsZero() bool { return a.Seq == 0 && a.Site == "" }
