package nodes

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/tidwall/gjson"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/GMWalletApp/ChainScanner/internal/chains"
	"github.com/GMWalletApp/ChainScanner/internal/engine"
)

func TestFallbackPairsThroughAdminAndStrictAPI(t *testing.T) {
	store, err := engine.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	r, err := NewNodeRegistry(t.Context(), store, []chains.Network{{Name: "base"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = r.db.Exec("UPDATE rpc_nodes SET removed=1"); err != nil {
		t.Fatal(err)
	}
	const admin = "an-admin-token-for-fallback-test"
	const client = "a-consumer-token-for-fallback-test"
	handler, err := r.Handler(map[string]string{"app": client}, admin)
	if err != nil {
		t.Fatal(err)
	}
	call := func(method, path, body, token string, status int) *httptest.ResponseRecorder {
		t.Helper()
		req := httptest.NewRequest(method, path, strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+token)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)
		if w.Code != status {
			t.Fatalf("%s %s: %d %s", method, path, w.Code, w.Body.String())
		}
		return w
	}
	for _, host := range []string{"regular.example", "paid-one.example", "paid-two.example"} {
		path := "/v1/admin/nodes"
		if host != "regular.example" {
			path += "/fallback"
		}
		call("POST", path, `{"network":"base","url":"https://`+host+`","protocol":"http"}`, admin, 201)
	}
	r.lookup.probe = func(context.Context, chains.Network, bool) error { return nil }
	sample, err := os.ReadFile("../chains/testdata/strict/evm.json")
	if err != nil {
		t.Fatal(err)
	}
	hash := gjson.GetBytes(sample, "transaction.hash").Str
	var mu sync.Mutex
	var calls []string
	var fallbackErr error
	var fallbackBody json.RawMessage = sample
	r.lookup.read = func(_ context.Context, n chains.Network, h string, _ bool) (json.RawMessage, error) {
		mu.Lock()
		defer mu.Unlock()
		if h != hash {
			t.Errorf("unexpected hash %s", h)
		}
		calls = append(calls, n.URL)
		if strings.Contains(n.URL, "paid-") {
			return fallbackBody, fallbackErr
		}
		return sample, nil
	}
	path := "/v1/transactions/base/" + hash + "/strict"
	check := func(suffix, mode string, wantCode, wantCalls int) {
		t.Helper()
		calls = nil
		w := call("GET", path+suffix, "", client, wantCode)
		if len(calls) != wantCalls || gjson.Get(w.Body.String(), "verification").Str != mode {
			t.Fatalf("%s: calls %v, response %s", suffix, calls, w.Body.String())
		}
		if w.Code == 200 && gjson.Get(w.Body.String(), "result.transaction.hash").Str != hash {
			t.Fatal("wrong transaction", w.Body.String())
		}
	}
	check("", "fallback_rpc", 200, 1)
	w := call("GET", path+"?rounds=1", "", client, 200)
	source := w.Header().Get("X-ChainScanner-Pair")
	if len(source) != 64 || w.Header().Get("X-ChainScanner-Strict-Rounds") != "1" {
		t.Fatal(w.Header())
	}
	calls = nil
	retry := httptest.NewRequest("GET", path+"?rounds=1", nil)
	retry.Header.Set("Authorization", "Bearer "+client)
	retry.Header.Set("X-ChainScanner-Exclude", source)
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, retry)
	if w.Code != 200 || len(calls) != 2 || gjson.Get(w.Body.String(), "verification").Str != "two_providers" || strings.Contains(w.Header().Get("X-ChainScanner-Pair"), source) {
		t.Fatal(w.Code, w.Header(), calls)
	}
	check("?fallback_only=true", "fallback_rpc", 200, 1)
	if !strings.Contains(calls[0], "paid-") {
		t.Fatal(calls)
	}
	call("PUT", "/v1/admin/blacklist/keyword:paid-two", "", admin, 204)
	check("?fallback_only=true", "fallback_rpc", 200, 1)
	// Without another usable fallback, fallback-only cannot form a pair.
	fallbackBody = json.RawMessage(`{"receipt":null}`)
	check("?fallback_only=true&rounds=1", "", 503, 1)
	// Two regular endpoints can still verify a payment after one fallback
	// returns pending, malformed data, an RPC error or a missing transaction.
	call("POST", "/v1/admin/nodes", `{"network":"base","url":"https://regular-two.example","protocol":"http"}`, admin, 201)
	for _, tc := range []struct {
		raw json.RawMessage
		err error
	}{
		{fallbackBody, nil}, {json.RawMessage(`{}`), nil}, {nil, errors.New("upstream unavailable")}, {nil, chains.ErrTransactionMissing},
	} {
		fallbackBody, fallbackErr = tc.raw, tc.err
		check("?rounds=1", "two_providers", 200, 3)
		if !strings.Contains(calls[0], "paid-one") || calls[1] == calls[2] || !strings.Contains(calls[1], "regular") || !strings.Contains(calls[2], "regular") {
			t.Fatal(calls)
		}
	}
	// An ordinary request continues to use one regular endpoint.
	w = call("GET", strings.TrimSuffix(path, "/strict"), "", client, 200)
	if gjson.Get(w.Body.String(), "verification").Exists() || !strings.Contains(calls[len(calls)-1], "regular") {
		t.Fatal(w.Body, calls)
	}

}

func TestRepairsRotateIndependentlyOfFreshBlocks(t *testing.T) {
	p := &SourcePool{now: time.Now}
	for _, role := range []bool{false, true, true} {
		p.nodes = append(p.nodes, &endpoint{fallback: role, tip: 100})
	}
	eligible := func(*endpoint) bool { return false }
	for i := range 6 {
		ordinary, _ := p.pickRange(chains.Span{Last: 1}, 0, nil, eligible)
		repair, _ := p.pickRange(chains.Span{Last: 1, Failed: true}, 0, nil, eligible)
		if ordinary != p.nodes[0] || repair != p.nodes[1+i%2] {
			t.Fatalf("unexpected assignment on repair %d", i)
		}
	}
}

func TestTONFallbackIndexesVoteWithoutPublicLocator(t *testing.T) {
	store, err := engine.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx := t.Context()
	r, err := NewNodeRegistry(ctx, store, []chains.Network{{Name: "ton"}})
	if err != nil {
		t.Fatal(err)
	}
	for _, host := range []string{"a.example", "b.example"} {
		n, err := r.add(ctx, NodeOptions{NodeEndpoint: NodeEndpoint{"ton", "toncenter-v3", "https://" + host}}, true)
		if err != nil || n.Scannable {
			t.Fatal(n, err)
		}
	}
	nodes, err := r.lookupCandidates(ctx, "ton", true)
	if err != nil {
		t.Fatal(err)
	}
	r.lookup.probe = func(context.Context, chains.Network, bool) error { return nil }
	group, _ := r.tonGroup(ctx, nodes, map[string]bool{}, "unused-hash")
	if len(group) != 2 {
		t.Fatal("missing funded TON pair")
	}
	for _, v := range group {
		v.release()
		if !v.fallback || v.cfg.Protocol != "toncenter-v3" {
			t.Fatal(v.cfg)
		}
	}
}
