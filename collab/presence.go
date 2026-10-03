package collab

import "time"

// Presence 描述一个参与者的在线状态。这是临时状态：不写入文档历史、
// 不进版本向量、不被快照持久化，仅在参与者之间广播。
type Presence struct {
	ClientID ClientID
	Name     string
	Cursor   Anchor // 光标位置（锚点，随文档编辑稳定移动）
	SelStart Anchor // 选区起点
	SelEnd   Anchor // 选区终点
	Online   bool
	LastSeen time.Time
}

// PresenceEvent 是在线状态的变更事件。
type PresenceEvent struct {
	Join     bool
	Presence *Presence
}
