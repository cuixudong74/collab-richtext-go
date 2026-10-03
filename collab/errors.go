package collab

import "errors"

// 传输层错误。
var (
	ErrTransportClosed = errors.New("transport closed")
	ErrPeerNotFound    = errors.New("peer not found")
)
