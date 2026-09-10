package cli

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestCompletionScriptsRepresentEveryPublicCommand(t *testing.T) {
	t.Parallel()
	paths := publicCommandPaths(reflect.TypeFor[commandLine](), nil)
	for _, scriptPath := range []string{
		filepath.Join("..", "..", "contrib", "completions", "boomerangz.bash"),
		filepath.Join("..", "..", "contrib", "completions", "_boomerangz"),
	} {
		body, err := os.ReadFile(scriptPath)
		if err != nil {
			t.Fatal(err)
		}
		for _, path := range paths {
			quoted := "'" + strings.Join(path, " ") + "'"
			quotedTopLevel := "'" + strings.Join(path, " ") + " '"
			if !strings.Contains(string(body), quoted) && !strings.Contains(string(body), quotedTopLevel) {
				t.Errorf("%s does not represent command %q", scriptPath, strings.Join(path, " "))
			}
		}
	}

	fishPath := filepath.Join("..", "..", "contrib", "completions", "boomerangz.fish")
	fishBody, err := os.ReadFile(fishPath)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(string(fishBody), "\n")
	for _, path := range paths {
		marker := "__fish_use_subcommand"
		if len(path) > 1 {
			marker = "__boomerangz_needs_subcommand " + path[0]
		}
		if !fishLineContainsCandidate(lines, marker, path[len(path)-1]) {
			t.Errorf("%s does not represent command %q", fishPath, strings.Join(path, " "))
		}
	}
}

func fishLineContainsCandidate(lines []string, marker, candidate string) bool {
	for _, line := range lines {
		if !strings.Contains(line, marker) {
			continue
		}
		start := strings.LastIndex(line, "-a '")
		if start < 0 {
			continue
		}
		values := line[start+len("-a '"):]
		end := strings.IndexByte(values, '\'')
		if end < 0 {
			continue
		}
		for _, value := range strings.Fields(values[:end]) {
			if value == candidate {
				return true
			}
		}
	}
	return false
}

func publicCommandPaths(command reflect.Type, prefix []string) [][]string {
	var paths [][]string
	for i := range command.NumField() {
		field := command.Field(i)
		if _, commandField := field.Tag.Lookup("cmd"); !commandField {
			continue
		}
		if _, hidden := field.Tag.Lookup("hidden"); hidden {
			continue
		}
		name := field.Tag.Get("name")
		if name == "" {
			name = strings.ToLower(field.Name)
		}
		path := append(append([]string(nil), prefix...), name)
		children := publicCommandPaths(field.Type, path)
		if len(children) == 0 {
			paths = append(paths, path)
		} else {
			paths = append(paths, children...)
		}
	}
	return paths
}
