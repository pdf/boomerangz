package policy

import (
	"fmt"
	"maps"
	"slices"
	"strings"
	"unicode"

	"github.com/pdf/boomerangz/internal/zfs"
)

// Namespace and StateNamespace distinguish public policy from recovery metadata.
const (
	Namespace      = "org.boomerangz:"
	StateNamespace = Namespace + "state:"
)

// Value describes a resolved value and the local dataset that supplied it.
// An empty Dataset indicates an application default.
type Value struct {
	Value   string `json:"value"`
	Dataset string `json:"dataset,omitempty"`
}

// Flags exposes requested and effective send behavior independently.
type Flags struct {
	LargeBlocks  bool `json:"large_blocks"`
	Compressed   bool `json:"compressed"`
	EmbeddedData bool `json:"embedded_data"`
	Raw          bool `json:"raw"`
	Props        bool `json:"props"`
	Replicate    bool `json:"replicate"`
}

// Effective is a detached inspection result. Errors invalidate only this dataset.
type Effective struct {
	Enabled          bool              `json:"enabled"`
	Grid             Grid              `json:"grid"`
	Remote           []string          `json:"remote,omitempty"`
	Local            []string          `json:"local,omitempty"`
	Incremental      string            `json:"incremental"`
	Requested        Flags             `json:"requested"`
	Send             Flags             `json:"effective_send"`
	SetProperties    map[string]string `json:"set_properties,omitempty"`
	IgnoreProperties []string          `json:"ignore_properties,omitempty"`
	Discard          Discard           `json:"discard"`
	Values           map[string]Value  `json:"values"`
	Warnings         []string          `json:"warnings,omitempty"`
	Errors           []string          `json:"errors,omitempty"`
}

// Valid reports whether the policy is safe to use.
func (p Effective) Valid() bool { return len(p.Errors) == 0 }

// Clone detaches all mutable inspection fields.
func (p Effective) Clone() Effective {
	p.Remote = slices.Clone(p.Remote)
	p.Local = slices.Clone(p.Local)
	p.Values = maps.Clone(p.Values)
	p.SetProperties = maps.Clone(p.SetProperties)
	p.IgnoreProperties = slices.Clone(p.IgnoreProperties)
	p.Warnings = slices.Clone(p.Warnings)
	p.Errors = slices.Clone(p.Errors)
	return p
}

// Resolve inherits only locally configured public values. Received values and
// internal state never enter the effective map, including on promoted datasets.
func Resolve(dataset zfs.Dataset, parent *Effective, stored []zfs.Property, remotes map[string]struct{}) Effective {
	p := Effective{Values: make(map[string]Value), SetProperties: make(map[string]string)}
	if parent != nil {
		for key, value := range parent.Values {
			if value.Dataset != "" {
				p.Values[key] = value
			}
		}
	}
	for _, property := range stored {
		if property.Dataset == dataset.Name && property.Source == zfs.SourceLocal && IsPublic(property.Name) {
			p.Values[property.Name] = Value{Value: property.Value, Dataset: dataset.Name}
		}
	}
	encrypted := dataset.EncryptionRoot != "" && dataset.EncryptionRoot != "-"
	defaults := map[string]string{
		"enabled": "off", "policy": DefaultGrid, "large_blocks": "on", "compressed": "on",
		"raw": "off", "props": "off", "incremental": "all", "replicate": "off", "discard": "off",
	}
	if encrypted {
		defaults["raw"] = "on"
	}
	for key, value := range defaults {
		if _, exists := p.Values[Namespace+key]; !exists {
			p.Values[Namespace+key] = Value{Value: value}
		}
	}
	for key := range p.Values {
		name := strings.TrimPrefix(key, Namespace)
		_, known := defaults[name]
		if !known && name != "remote" && name != "local" && !strings.HasPrefix(name, "set_prop:") && !strings.HasPrefix(name, "ignore_prop:") {
			p.Errors = append(p.Errors, "unknown public property "+key)
		}
	}
	p.Enabled = p.toggle("enabled")
	var err error
	p.Grid, err = ParseGrid(p.Values[Namespace+"policy"].Value)
	if err != nil {
		p.Errors = append(p.Errors, "policy: "+err.Error())
	}
	p.Remote = p.targets("remote")
	p.Local = p.targets("local")
	for _, remote := range p.Remote {
		if _, exists := remotes[remote]; !exists {
			p.Errors = append(p.Errors, fmt.Sprintf("unknown remote %q", remote))
		}
	}
	for _, local := range p.Local {
		if err := zfs.ValidateDataset(local); err != nil {
			p.Errors = append(p.Errors, "local: "+err.Error())
		}
	}
	p.Incremental = p.Values[Namespace+"incremental"].Value
	if p.Incremental != "all" && p.Incremental != "latest" {
		p.Errors = append(p.Errors, "incremental must be all or latest")
	}
	p.Requested = Flags{LargeBlocks: p.toggle("large_blocks"), Compressed: p.toggle("compressed"), Raw: p.toggle("raw"), Props: p.toggle("props"), Replicate: p.toggle("replicate")}
	p.Send = p.Requested
	if p.Send.Replicate {
		p.Send.Props = true
	}
	if p.Send.Raw && !encrypted {
		p.Send.LargeBlocks, p.Send.Compressed, p.Send.EmbeddedData = true, true, true
		if !p.Requested.LargeBlocks || !p.Requested.Compressed {
			p.Warnings = append(p.Warnings, "raw sends of unencrypted data imply large_blocks and compressed")
		}
	}
	if p.Send.Replicate && encrypted && !p.Send.Raw {
		p.Errors = append(p.Errors, "encrypted recursive replication requires raw=on")
	}
	p.Discard = Discard(p.Values[Namespace+"discard"].Value)
	if p.Discard != DiscardOff && p.Discard != DiscardFirst && p.Discard != DiscardAll {
		p.Errors = append(p.Errors, "discard must be off, first, or all")
	}
	p.receiveProperties(encrypted)
	slices.Sort(p.Errors)
	slices.Sort(p.Warnings)
	return p
}

// IsPublic includes unknown public keys, allowing future receive exclusion inventories.
func IsPublic(name string) bool {
	return strings.HasPrefix(name, Namespace) && !strings.HasPrefix(name, StateNamespace)
}

func (p *Effective) toggle(key string) bool {
	value := p.Values[Namespace+key].Value
	if value != "on" && value != "off" {
		p.Errors = append(p.Errors, key+" must be on or off")
	}
	return value == "on"
}

// ParseTargets trims and deduplicates a nonempty comma-separated target list.
func ParseTargets(value string) ([]string, error) {
	var targets []string
	seen := make(map[string]bool)
	for _, target := range strings.Split(value, ",") {
		target = strings.TrimSpace(target)
		if target == "" {
			return nil, fmt.Errorf("target list contains an empty entry")
		}
		if !seen[target] {
			targets = append(targets, target)
			seen[target] = true
		}
	}
	return targets, nil
}

func (p *Effective) targets(key string) []string {
	value, exists := p.Values[Namespace+key]
	if !exists {
		return nil
	}
	result, err := ParseTargets(value.Value)
	if err != nil {
		p.Errors = append(p.Errors, key+": "+err.Error())
	}
	return result
}

func (p *Effective) receiveProperties(encrypted bool) {
	for _, key := range slices.Sorted(maps.Keys(p.Values)) {
		name := strings.TrimPrefix(key, Namespace)
		kind, target, dynamic := strings.Cut(name, ":")
		if !dynamic || (kind != "set_prop" && kind != "ignore_prop") {
			continue
		}
		if target == "" || strings.HasPrefix(target, Namespace) || strings.ContainsAny(target, "=") || strings.ContainsFunc(target, func(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) }) {
			p.Errors = append(p.Errors, "invalid or reserved receive property: "+target)
			continue
		}
		if kind == "set_prop" {
			p.SetProperties[target] = p.Values[key].Value
		} else if p.toggle(name) {
			p.IgnoreProperties = append(p.IgnoreProperties, target)
		}
	}
	p.IgnoreProperties = slices.DeleteFunc(p.IgnoreProperties, func(key string) bool {
		if _, exists := p.SetProperties[key]; exists {
			p.Warnings = append(p.Warnings, "set_prop wins over ignore_prop for "+key)
			return true
		}
		return false
	})
	if p.Send.Raw && encrypted {
		p.validateRawReceive()
	}
}

func (p *Effective) validateRawReceive() {
	for _, key := range []string{"encryption", "keyformat", "pbkdf2iters"} {
		// keylocation can be chosen at receive time; the encryption and key
		// derivation parameters are fixed by an encrypted raw stream.
		if _, exists := p.SetProperties[key]; exists {
			p.Errors = append(p.Errors, "raw encrypted stream cannot override "+key)
		}
		if slices.Contains(p.IgnoreProperties, key) {
			p.Errors = append(p.Errors, "raw encrypted stream cannot exclude "+key)
		}
	}
}

// ForReplicationScope accounts for an encrypted descendant of a replication
// root. Only an unspecified raw setting may be selected automatically; explicit
// local or inherited choices retain their authority. The result is detached.
func (p Effective) ForReplicationScope(rootEncrypted bool, encryptedDescendant string) Effective {
	p = p.Clone()
	if !p.Send.Replicate || encryptedDescendant == "" {
		return p
	}
	if !p.Send.Raw {
		if p.Values[Namespace+"raw"].Dataset != "" {
			p.Errors = append(p.Errors, "encrypted replication descendant "+encryptedDescendant+" requires raw=on; explicit raw setting forbids automatic selection")
			return p
		}
		p.Send.Raw = true
		p.Warnings = append(p.Warnings, "raw enabled automatically for encrypted replication descendant "+encryptedDescendant)
		if !rootEncrypted {
			p.Send.LargeBlocks, p.Send.Compressed, p.Send.EmbeddedData = true, true, true
			if !p.Requested.LargeBlocks || !p.Requested.Compressed {
				p.Warnings = append(p.Warnings, "raw sends of unencrypted data imply large_blocks and compressed")
			}
		}
	}
	// Receive overrides apply across the package, including encrypted children
	// below an unencrypted root and packages with explicitly requested raw mode.
	p.validateRawReceive()
	slices.Sort(p.Errors)
	p.Errors = slices.Compact(p.Errors)
	slices.Sort(p.Warnings)
	p.Warnings = slices.Compact(p.Warnings)
	return p
}
