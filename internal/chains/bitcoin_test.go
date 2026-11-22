package chains

import (
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/GMWalletApp/ChainScanner/internal/testutil"
)

func TestBitcoinScanSpendChangeAndIncompletePrevout(t *testing.T) {
	type obj = map[string]any
	hash := strings.Repeat("b", 64)
	id := strings.Repeat("a", 64)
	script := obj{"hex": "76a914" + strings.Repeat("00", 20) + "88ac"}
	recipient := obj{"hex": "0014" + strings.Repeat("11", 20)}
	prev := obj{"txid": id, "vout": 0, "prevout": obj{"scriptPubKey": script}}
	spend := obj{"txid": id, "vin": []obj{prev}, "vout": []obj{{"scriptPubKey": recipient}, {"scriptPubKey": script}}}
	coinbase := obj{"txid": strings.Repeat("1", 64), "vin": []obj{{"coinbase": "00"}}, "vout": []obj{{"scriptPubKey": obj{"hex": "6a"}}}}
	block := obj{"height": 1, "hash": hash, "previousblockhash": bitcoinGenesis, "time": 12345, "nTx": 2, "tx": []obj{coinbase, spend}}
	srv := testutil.RPCServer(t, func(r testutil.RPCRequest) (any, error) {
		switch r.Method {
		case "getblockhash":
			if string(r.Params[0]) == "0" {
				return bitcoinGenesis, nil
			}
			return hash, nil
		case "getblock":
			if string(r.Params[1]) != "3" {
				t.Error("missing prevouts")
			}
			return block, nil
		}
		return nil, errors.New("unexpected method")
	})
	defer srv.Close()
	s, e := NewSource(t.Context(), Network{Name: "bitcoin", URL: srv.URL})
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	_, link, rows, e := ReadSpan(t.Context(), "bitcoin", s, Span{First: 1, Last: 1})
	if e != nil || link.Parent != bitcoinGenesis || link.Hash != hash || len(rows) != 2 || len(rows[0].Addresses) != 0 || len(rows[1].Addresses) != 2 || !slices.Contains(rows[1].Addresses, "1111111111111111111114oLvT2") {
		t.Fatal(link, rows, e)
	}
	delete(prev, "prevout")
	if _, _, e := s.Read(t.Context(), 1, 1); e == nil {
		t.Fatal("incomplete spent output advanced scan")
	}
}

func TestBitcoinAddressNetworkAndCase(t *testing.T) {
	const a = "bc1qzyg3zyg3zyg3zyg3zyg3zyg3zyg3zyg3h8ffkz"
	if v, e := Canonical("bitcoin", strings.ToUpper(a)); e != nil || v != a {
		t.Fatal(v, e)
	}
	for _, bad := range []string{"mipcBbFg9gMiCh81Kj8tqqdgoZub1ZJRfn", "bc1Q" + a[4:], "1111111111111111111114oLvT3"} {
		if _, e := Canonical("bitcoin", bad); e == nil {
			t.Fatal(bad)
		}
	}
}
