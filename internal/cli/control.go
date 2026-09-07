package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/pdf/boomerangz/internal/config"
	"github.com/pdf/boomerangz/internal/control"
	controlrpc "github.com/pdf/boomerangz/internal/control/rpc"
	"github.com/pdf/boomerangz/internal/statusui"
	"golang.org/x/term"
)

func controlClient(ctx context.Context, cfg config.Config, credential string) (*control.Client, error) {
	if credential == "" {
		return control.DialLocal(ctx, cfg.Paths.SocketPath)
	}
	if !filepath.IsAbs(credential) {
		credential = filepath.Join(cfg.Paths.CredentialsDir, credential+".json")
	}
	bundle, err := control.LoadPairingBundle(credential)
	if err != nil {
		return nil, err
	}
	return control.DialBundle(ctx, bundle)
}

func terminalWidth(writer io.Writer) (bool, int) {
	file, ok := writer.(*os.File)
	if !ok || !term.IsTerminal(int(file.Fd())) {
		return false, 0
	}
	width, _, err := term.GetSize(int(file.Fd()))
	if err != nil {
		width = 80
	}
	return true, width
}

func runStatus(ctx context.Context, out io.Writer, cfg config.Config, credential string, watch bool, interval time.Duration) error {
	if interval < 100*time.Millisecond || interval > time.Hour {
		return fmt.Errorf("status interval must be between 100ms and 1h")
	}
	client, err := controlClient(ctx, cfg, credential)
	if err != nil {
		return err
	}
	defer func() { _ = client.Connection.Close() }()
	interactive, width := terminalWidth(out)
	if !watch {
		response, err := client.Status.GetStatus(ctx, &controlrpc.GetStatusRequest{})
		if err != nil {
			return err
		}
		if interactive {
			return statusui.Terminal(out, response.GetStatus(), width)
		}
		return statusui.JSON(out, response.GetStatus())
	}
	stream, err := client.Status.WatchStatus(ctx, &controlrpc.WatchStatusRequest{IntervalMilliseconds: uint32(interval.Milliseconds())})
	if err != nil {
		return err
	}
	for {
		response, receiveErr := stream.Recv()
		if receiveErr != nil {
			if errors.Is(receiveErr, io.EOF) || ctx.Err() != nil {
				return nil
			}
			return receiveErr
		}
		if interactive {
			if _, err := fmt.Fprint(out, "\x1b[H\x1b[2J"); err != nil {
				return err
			}
			_, width = terminalWidth(out)
			if err := statusui.Terminal(out, response.GetStatus(), width); err != nil {
				return err
			}
		} else if err := statusui.JSON(out, response.GetStatus()); err != nil {
			return err
		}
	}
}

func runTrigger(ctx context.Context, out io.Writer, cfg config.Config, credential string, datasets []string) error {
	client, err := controlClient(ctx, cfg, credential)
	if err != nil {
		return err
	}
	defer func() { _ = client.Connection.Close() }()
	response, err := client.Control.Trigger(ctx, &controlrpc.TriggerRequest{Datasets: datasets})
	if err != nil {
		return err
	}
	return json.NewEncoder(out).Encode(struct {
		Accepted []string `json:"accepted"`
	}{response.GetAccepted()})
}

func runDaemonClean(ctx context.Context, out io.Writer, cfg config.Config, names []string, recursive, all, destroy, apply bool) (bool, error) {
	_, err := os.Lstat(cfg.Paths.SocketPath)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return true, err
	}
	client, err := control.DialLocal(ctx, cfg.Paths.SocketPath)
	if err != nil {
		return true, fmt.Errorf("connect to daemon control socket: %w", err)
	}
	defer func() { _ = client.Connection.Close() }()
	response, err := client.Control.Clean(ctx, &controlrpc.CleanRequest{Datasets: names, Recursive: recursive, All: all, DestroyOwnedSnapshots: destroy, Apply: apply})
	if err != nil {
		return true, err
	}
	type action struct {
		Operation string `json:"operation"`
		Object    string `json:"object"`
		Property  string `json:"property,omitempty"`
		GUID      uint64 `json:"guid,omitempty"`
	}
	type options struct {
		Recursive bool `json:"recursive"`
		Destroy   bool `json:"destroy_owned_snapshots"`
	}
	type plan struct {
		Dataset  string   `json:"dataset"`
		Options  options  `json:"options"`
		Actions  []action `json:"actions"`
		Blockers []string `json:"blockers"`
		Warnings []string `json:"warnings"`
		Applied  uint32   `json:"applied"`
	}
	plans := make([]plan, 0, len(response.GetPlans()))
	for _, source := range response.GetPlans() {
		item := plan{Dataset: source.GetDataset(), Options: options{Recursive: source.GetRecursive(), Destroy: source.GetDestroyOwnedSnapshots()}, Blockers: source.GetBlockers(), Warnings: source.GetWarnings(), Applied: source.GetApplied()}
		for _, sourceAction := range source.GetActions() {
			item.Actions = append(item.Actions, action{sourceAction.GetOperation(), sourceAction.GetObject(), sourceAction.GetProperty(), sourceAction.GetGuid()})
		}
		plans = append(plans, item)
	}
	encoder := json.NewEncoder(out)
	encoder.SetIndent("", "  ")
	encodeErr := encoder.Encode(plans)
	if response.GetError() != "" {
		return true, errors.Join(errors.New(response.GetError()), encodeErr)
	}
	return true, encodeErr
}

func selectPairingListener(cfg config.Config, name string) (string, config.ListenerConfig, error) {
	if name != "" {
		listener, exists := cfg.Listeners[name]
		if !exists {
			return "", config.ListenerConfig{}, fmt.Errorf("listener %s was not found", name)
		}
		if listener.Network != "tcp" {
			return "", config.ListenerConfig{}, fmt.Errorf("listener %s is not a TCP listener", name)
		}
		return name, listener, nil
	}
	for candidate, listener := range cfg.Listeners {
		if listener.Network == "tcp" {
			if name != "" {
				return "", config.ListenerConfig{}, fmt.Errorf("multiple token listeners exist; select one with --listener")
			}
			name = candidate
		}
	}
	if name == "" {
		return "", config.ListenerConfig{}, fmt.Errorf("no TCP listener is configured")
	}
	return name, cfg.Listeners[name], nil
}

func runPairingCreate(out io.Writer, cfg config.Config, listenerName, clientCert, clientKey string, scopes []string, expiry time.Duration) error {
	name, listener, err := selectPairingListener(cfg, listenerName)
	if err != nil {
		return err
	}
	var expires *time.Time
	if expiry < 0 {
		return fmt.Errorf("token expiry cannot be negative")
	}
	if expiry > 0 {
		value := time.Now().UTC().Add(expiry)
		expires = &value
	}
	store, err := control.NewTokenStore(cfg.Paths.IdentityDir)
	if err != nil {
		return err
	}
	bundle, err := control.CreateListenerPairing(store, cfg.Paths.IdentityDir, name, listener, clientCert, clientKey, scopes, expires)
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(out)
	encoder.SetIndent("", "  ")
	return encoder.Encode(bundle)
}

func runPairingImport(out io.Writer, cfg config.Config, name, path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	stored, err := control.ImportPairingBundle(cfg.Paths.CredentialsDir, name, data)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(out, stored)
	return err
}

func runTokenList(out io.Writer, cfg config.Config) error {
	store, err := control.NewTokenStore(cfg.Paths.IdentityDir)
	if err != nil {
		return err
	}
	tokens, err := store.List()
	if err != nil {
		return err
	}
	return json.NewEncoder(out).Encode(tokens)
}

func runTokenRevoke(out io.Writer, cfg config.Config, id string) error {
	store, err := control.NewTokenStore(cfg.Paths.IdentityDir)
	if err != nil {
		return err
	}
	if err := store.Revoke(id); err != nil {
		return err
	}
	_, err = fmt.Fprintf(out, "revoked token %s\n", id)
	return err
}
