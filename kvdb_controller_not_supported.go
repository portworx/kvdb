package kvdb

var (
	// KvdbControllerNotSupported is a null controller implementation. This can be used
	// kvdb implementors that do no want to implement the controller interface
	KvdbControllerNotSupported = &controllerNotSupported{}
)

type controllerNotSupported struct{}

func (c *controllerNotSupported) AddMember(nodeIP, nodePeerPort, nodeName string) (map[string][]string, error) {
	return nil, ErrNotSupported
}

func (c *controllerNotSupported) RemoveMember(nodeID string) error {
	return ErrNotSupported
}

func (c *controllerNotSupported) ListMembers() (map[string]*MemberUrls, error) {
	return nil, ErrNotSupported
}

func (c *controllerNotSupported) SetEndpoints(endpoints []string) error {
	return ErrNotSupported
}

func (c *controllerNotSupported) GetEndpoints() []string {
	return []string{}
}
