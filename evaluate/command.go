package evaluate

type Commander interface {
	Command(ses *Session) error
}
