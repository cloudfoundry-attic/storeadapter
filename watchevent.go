package storeadapter

type WatchEvent struct {
	Type EventType
	Node StoreNode
}

type EventType int

const (
	InvalidEvent = EventType(iota)
	CreateEvent
	DeleteEvent
	ExpireEvent
	UpdateEvent
	GetEvent
)
