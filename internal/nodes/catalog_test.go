package nodes

import (
	"context"
	"strings"
	"testing"

	"github.com/GMWalletApp/ChainScanner/internal/chains"
	"github.com/GMWalletApp/ChainScanner/internal/engine"
)

func TestBuiltinRoutesAndHeightOnlyNodes(t *testing.T) {
	ctx := context.Background()
	store, err := engine.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	r, err := NewNodeRegistry(ctx, store, nil)
	if err != nil {
		t.Fatal(err)
	}
	list, err := r.List(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	seen := map[NodeEndpoint]bool{}
	for _, n := range list {
		key := n.NodeEndpoint.identity()
		if seen[key] {
			t.Fatalf("duplicate %v", n.NodeEndpoint)
		}
		seen[key] = true
		if n.Scannable {
			if err := chains.ValidateNetwork(r.network(n, chains.Network{})); err != nil {
				t.Fatal(n.NodeEndpoint, err)
			}
		}
	}
	for _, e := range []NodeEndpoint{
		{"solana", "http", "https://api.mainnet-beta.solana.com"},
		{"solana", "http", "https://solana-rpc.publicnode.com"},
		{"solana", "http", "https://public.rpc.solanavibestation.com"},
		{"solana", "ws", "wss://solana-rpc.publicnode.com"},
	} {
		if !seen[e] {
			t.Fatal("missing", e)
		}
	}
	for _, e := range heightOnlyNodes {
		if !seen[e] {
			t.Fatal("missing", e)
		}
		pool := newRegistryPool(ctx, r, chains.Network{Name: e.Network})
		err := pool.refresh(ctx)
		pool.Close()
		if err != nil {
			t.Fatal(err)
		}
		for cfg := range pool.inventory {
			if cfg.URL == e.URL {
				t.Fatal("read lane for height-only node", cfg)
			}
		}
	}
	for _, n := range list {
		if n.Network == "bitcoin" && n.Scannable {
			t.Fatal(n.NodeEndpoint)
		}
		if strings.Contains(n.URL, "yellowstone-grpc.publicnode.com") {
			t.Fatal("token required", n.NodeEndpoint)
		}
	}
}

// Check the source list before SQLite's unique constraint can hide repeated entries.
func TestBuiltinCatalogIsUnique(t *testing.T) {
	seen := map[NodeEndpoint]NodeEndpoint{}
	for _, e := range builtinNodes {
		key := e.identity()
		if original, ok := seen[key]; ok {
			t.Errorf("%v duplicates %v", e, original)
		}
		seen[key] = e
	}
}

func TestAptosLabsHostsUseSeparateReadLanes(t *testing.T) {
	pool := NewSourcePool(context.Background(), "aptos")
	defer pool.Close()
	err := pool.SetNodes([]chains.Network{
		{Name: "aptos", URL: "https://api.mainnet.aptoslabs.com/v1"},
		{Name: "aptos", URL: "https://fullnode.mainnet.aptoslabs.com/v1"},
		{Name: "aptos", URL: "https://api.mainnet.aptoslabs.com:443/v1/"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(pool.nodes) != 2 || pool.nodes[0].historyKey == pool.nodes[1].historyKey {
		t.Fatal("expected two distinct Aptos Labs read lanes")
	}
}

func TestPublicNodeHistoryLane(t *testing.T) {
	if historyLane("base", "wss://base-rpc.publicnode.com:443/") != historyLane("base", "https://base-rpc.publicnode.com") {
		t.Fatal("duplicate history lanes")
	}
	if historyLane("base", "wss://other.example") == historyLane("base", "https://other.example") {
		t.Fatal("changed custom routes")
	}
}
