package config

import "github.com/pelletier/go-toml/v2"

// MarshalRedacted returns effective TOML with secret fields removed.
func MarshalRedacted(config Config) ([]byte, error) {
	if config.Listeners == nil {
		return toml.Marshal(config)
	}
	redacted := config
	redacted.Listeners = make(map[string]ListenerConfig, len(config.Listeners))
	for name, listener := range config.Listeners {
		if listener.TLSKey != "" {
			listener.TLSKey = "<redacted>"
		}
		redacted.Listeners[name] = listener
	}
	return toml.Marshal(redacted)
}
