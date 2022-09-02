package redis

type lock interface {
	Lock() (bool, error)
	Unlock() error
}
