package crdt

// VersionVector 记录每个 Site 已见到的最大 Seq。
type VersionVector map[SiteID]uint64

func (v VersionVector) Clone() VersionVector {
	out := make(VersionVector, len(v))
	for k, val := range v {
		out[k] = val
	}
	return out
}

// Covers 报告 v 是否已包含 (site, seq)。
func (v VersionVector) Covers(site SiteID, seq uint64) bool {
	return v[site] >= seq
}
