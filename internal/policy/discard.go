package policy

// Discard controls receive-side path component removal.
type Discard string

// Valid discard choices correspond to no mapping flag, -d, and -e respectively.
const (
	DiscardOff   Discard = "off"
	DiscardFirst Discard = "first"
	DiscardAll   Discard = "all"
)
