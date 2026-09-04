package config

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/pelletier/go-toml/v2"
)

// Loaded contains effective configuration and source-file provenance.
type Loaded struct {
	Config     Config
	Sources    []string
	Provenance map[string]string
}

// Load reads the primary file and lexically ordered TOML drop-ins.
func Load(mainPath, dropInDir string) (Loaded, error) {
	paths := []string{mainPath}
	entries, err := os.ReadDir(dropInDir)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return Loaded{}, fmt.Errorf("read config directory %q: %w", dropInDir, err)
	}
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".toml") {
			paths = append(paths, filepath.Join(dropInDir, entry.Name()))
		}
	}
	sort.Strings(paths[1:])

	merged := make(map[string]any)
	provenance := make(map[string]string)
	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil {
			return Loaded{}, fmt.Errorf("read config %q: %w", path, err)
		}
		var document map[string]any
		decoder := toml.NewDecoder(bytes.NewReader(data))
		if err := decoder.Decode(&document); err != nil {
			return Loaded{}, fmt.Errorf("parse config %q: %w", path, err)
		}
		mergeMap(merged, document, "", path, provenance)
	}

	data, err := toml.Marshal(merged)
	if err != nil {
		return Loaded{}, fmt.Errorf("encode merged configuration: %w", err)
	}
	result := Defaults()
	decoder := toml.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&result); err != nil {
		return Loaded{}, fmt.Errorf("decode merged configuration: %w", err)
	}
	if err := result.Validate(); err != nil {
		return Loaded{}, err
	}
	return Loaded{Config: result, Sources: paths, Provenance: provenance}, nil
}

func mergeMap(dst, src map[string]any, prefix, source string, provenance map[string]string) {
	for key, value := range src {
		path := key
		if prefix != "" {
			path = prefix + "." + key
		}
		srcMap, srcOK := value.(map[string]any)
		dstMap, dstOK := dst[key].(map[string]any)
		if srcOK && dstOK {
			mergeMap(dstMap, srcMap, path, source, provenance)
			continue
		}
		dst[key] = value
		markProvenance(value, path, source, provenance)
	}
}

func markProvenance(value any, path, source string, provenance map[string]string) {
	if nested, ok := value.(map[string]any); ok {
		for key, child := range nested {
			markProvenance(child, path+"."+key, source, provenance)
		}
		return
	}
	provenance[path] = source
}
