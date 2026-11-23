package nodes

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"maps"
	"math/rand/v2"
	"slices"
	"sync"
	"time"

	"github.com/GMWalletApp/ChainScanner/internal/chains"
)

const strictGroupSize = 2

var errCapacity = errors.New("insufficient available transaction nodes")
var errAgreement = errors.New("transaction lookup failed")

type lookupCandidate struct {
	weight     int
	healthy    bool
	fallback   bool
	lastChoice bool
	peerID     string
	primaryURL string
	cfg        chains.Network
	public     bool
	version    nodeVersion
	key        string
}
type lookupPenalty struct {
	streak                   failureStreak
	retry, expires, verified time.Time
}
type transactionLookup struct {
	reserve   sync.Mutex
	lanes     providerLanes
	mu        sync.Mutex
	penalties map[nodeVersion]lookupPenalty
	nextSweep time.Time
	probe     func(context.Context, chains.Network, bool) error
	read      func(context.Context, chains.Network, string, bool) (json.RawMessage, error)
}

func (l *transactionLookup) ready(id nodeVersion, now time.Time) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return !now.Before(l.penalties[id].retry)
}
func (l *transactionLookup) record(id nodeVersion, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.penalties == nil {
		l.penalties = map[nodeVersion]lookupPenalty{}
	}
	now := time.Now()
	// Bound storage without scanning every provider while holding the lock on every reply.
	if !now.Before(l.nextSweep) {
		for key, p := range l.penalties {
			if !now.Before(p.expires) {
				delete(l.penalties, key)
			}
		}
		l.nextSweep = now.Add(time.Minute)
	}
	if err == nil || errors.Is(err, chains.ErrTransactionMissing) || errors.Is(err, chains.ErrTransactionPending) {
		l.penalties[id] = lookupPenalty{verified: now.Add(time.Minute), expires: now.Add(time.Minute)}
		return
	}
	p := l.penalties[id]
	if !now.Before(p.expires) {
		p = lookupPenalty{}
	}
	p.verified = time.Time{}
	p.retry = now.Add(p.streak.fail())
	p.expires = p.retry.Add(time.Hour)
	l.penalties[id] = p
}

func (r *NodeRegistry) lookupCandidates(ctx context.Context, network string, strict bool, manual ...bool) ([]lookupCandidate, error) {
	rows, err := r.List(ctx, network)
	if err != nil {
		return nil, err
	}
	now := time.Now()
	var candidates []lookupCandidate
	for _, n := range rows {
		if !n.Enabled || n.Purpose == "manual_verify" && !(strict && len(manual) > 0 && manual[0]) || n.Role == "fallback" && !strict {
			continue
		}
		if n.ReadRetryAt > now.UnixMilli() || (!n.Scannable && n.Protocol != "toncenter-v3") {
			continue
		}
		recent := n.CheckedAt != nil && now.Sub(time.UnixMilli(*n.CheckedAt)) < time.Minute
		if recent && n.Status != "ok" && n.Status != "untested" {
			continue
		}
		cfg, ok := chains.LookupRoute(r.network(n, chains.Network{}))
		host, peerID, peer := chains.TONVoter(cfg)
		if !ok && !(strict && peer) {
			continue
		}
		key := providerKey(cfg)
		if peer {
			key = "ton|" + host
		}
		candidates = append(candidates, lookupCandidate{
			healthy: recent && n.Status == "ok", fallback: n.Role == "fallback", weight: n.Weight,
			cfg: cfg, version: n.version(), public: isDiscoveryOrigin(n.Origin),
			key: key, peerID: peerID, primaryURL: n.URL,
		})
	}
	rand.Shuffle(len(candidates), func(i, j int) { candidates[i], candidates[j] = candidates[j], candidates[i] })

	slices.SortStableFunc(candidates, func(a, b lookupCandidate) int { return cmp.Compare(b.weight, a.weight) })
	return candidates, nil
}

type lookupLease struct {
	lookupCandidate
	release func()
}

func (l *transactionLookup) take(candidates []lookupCandidate, used map[string]bool, count int, allowBusy bool) []lookupLease {
	l.reserve.Lock()
	defer l.reserve.Unlock()
	var group []lookupLease
	for _, c := range candidates {
		if used[c.key] || c.peerID != "" && used[c.peerID] || !l.ready(c.version, time.Now()) {
			continue
		}
		if release, ok := l.lanes.tryLookup(c.key, allowBusy); ok {
			used[c.key] = true
			if c.peerID != "" {
				used[c.peerID] = true
			}
			group = append(group, lookupLease{c, release})
			if len(group) == count {
				return group
			}
		}
	}
	return group
}

type lookupChoices struct{ fresh, cold []lookupCandidate }

func (l *transactionLookup) rank(candidates []lookupCandidate) (ordinary, paid, last lookupChoices) {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := time.Now()
	for _, c := range candidates {
		dst := &ordinary
		switch {
		case c.lastChoice:
			dst = &last
		case c.fallback:
			dst = &paid
		}
		if c.healthy || now.Before(l.penalties[c.version].verified) {
			dst.fresh = append(dst.fresh, c)
		} else {
			dst.cold = append(dst.cold, c)
		}
	}
	return
}

func (l *transactionLookup) group(ctx context.Context, candidates []lookupCandidate, used map[string]bool, count int) []lookupLease {
	ctx, cancel := context.WithTimeout(ctx, 4*time.Second)
	defer cancel()
	ordinary, paid, last := l.rank(candidates)
	idleWindow := 4 * time.Second
	if count == strictGroupSize {
		idleWindow = 2 * time.Second
	}
	idleCtx, stop := context.WithTimeout(ctx, idleWindow)
	var group []lookupLease
	if count == strictGroupSize && len(paid.fresh)+len(paid.cold) > 0 {
		// Independent fallback providers may fill both voting slots.
		group = l.fill(idleCtx, paid.fresh, paid.cold, used, count, true)
	}
	group = append(group, l.fill(idleCtx, ordinary.fresh, ordinary.cold, used, count-len(group), false)...)
	stop()
	if count == strictGroupSize && len(group) < count {
		group = append(group, l.fill(ctx, ordinary.fresh, ordinary.cold, used, count-len(group), true)...)
		if len(group) < count {
			group = append(group, l.fill(ctx, paid.fresh, paid.cold, used, count-len(group), true)...)
		}
	}

	if count == strictGroupSize && len(group) < count {
		group = append(group, l.fill(ctx, last.fresh, last.cold, used, count-len(group), true)...)
	}
	if len(group) == count {
		return group
	}
	for _, lease := range group {
		lease.release()
	}
	return nil
}

func (l *transactionLookup) fill(ctx context.Context, fresh, cold []lookupCandidate, used map[string]bool, count int, allowBusy bool) []lookupLease {
	if count <= 0 {
		return nil
	}
	group := l.take(fresh, used, count, allowBusy)
	for len(group) < count && ctx.Err() == nil {
		batch := l.take(cold, used, count-len(group), allowBusy)
		if len(batch) == 0 {
			break
		}
		var wg sync.WaitGroup
		ok := make([]bool, len(batch))
		for i, lease := range batch {
			wg.Go(func() {
				attempt, stop := context.WithTimeout(ctx, 2*time.Second)
				defer stop()
				err := l.probe(attempt, lease.cfg, lease.public)
				if ctx.Err() == nil && err != nil {
					l.record(lease.version, err)
				}
				ok[i] = err == nil
			})
		}
		wg.Wait()
		for i, lease := range batch {
			if ok[i] {
				group = append(group, lease)
			} else {
				lease.release()
			}
		}
	}
	return group
}

type transactionReader func(context.Context, chains.Network, string, bool) (json.RawMessage, error)

func (l *transactionLookup) round(ctx context.Context, group []lookupLease, hash string, read transactionReader) (json.RawMessage, error) {
	ctx, cancel := context.WithTimeout(ctx, 9*time.Second)
	defer cancel()
	type result struct {
		value json.RawMessage
		err   error
	}
	results := make([]result, len(group))
	var wg sync.WaitGroup
	for i, lease := range group {
		wg.Go(func() {
			defer lease.release()
			v, err := read(ctx, lease.cfg, hash, lease.public)
			if ctx.Err() == nil || errors.Is(ctx.Err(), context.DeadlineExceeded) {
				l.record(lease.version, err)
			}
			results[i] = result{v, err}
		})
	}
	wg.Wait()
	if len(results) == 1 {
		return results[0].value, results[0].err
	}
	for _, r := range results {
		if r.err != nil {
			return nil, r.err
		}
		if len(r.value) == 0 {
			return nil, errAgreement
		}
	}
	return chains.AgreeTransaction(group[0].cfg, results[0].value, results[1].value)
}

// Prefer a pair of operator-selected voters. Otherwise use LiteServer proofs
// when available, with independent v3 indexes as an alternative.
func (r *NodeRegistry) tonGroup(ctx context.Context, candidates []lookupCandidate, used map[string]bool, hash string) ([]lookupLease, transactionReader) {
	var peers, indexes, fallbackIndexes []lookupCandidate
	paidPeers := 0
	for _, c := range candidates {
		if c.peerID != "" {
			peers = append(peers, c)
			if c.fallback {
				paidPeers++
			}
		} else {
			indexes = append(indexes, c)
			if c.fallback {
				fallbackIndexes = append(fallbackIndexes, c)
			}
		}
	}
	if paidPeers < strictGroupSize && len(fallbackIndexes) >= strictGroupSize {
		// An incomplete reservation must not exclude a healthy index from the mixed pair below.
		selected := maps.Clone(used)
		if group := r.lookup.group(ctx, fallbackIndexes, selected, strictGroupSize); len(group) == strictGroupSize {
			maps.Copy(used, selected)
			return group, r.lookup.read
		}
	}
	if len(peers) >= 2 {
		if read := r.tonProofReader(ctx, indexes, hash); read != nil {
			if group := r.lookup.group(ctx, peers, used, strictGroupSize); len(group) > 0 {
				return group, read
			}
		}
	}
	return r.lookup.group(ctx, indexes, used, strictGroupSize), r.lookup.read
}

// The index supplies a location hint; only the selected LiteServer supplies the proof.
func (r *NodeRegistry) tonProofReader(ctx context.Context, candidates []lookupCandidate, hash string) transactionReader {
	locate, stop := context.WithTimeout(ctx, 3*time.Second)
	defer stop()
	for _, idx := range candidates {
		if locate.Err() != nil {
			break
		}
		if idx.cfg.Protocol != "toncenter-v3" || !r.lookup.ready(idx.version, time.Now()) {
			continue
		}
		hint, err := chains.LocateTON(locate, idx.cfg, hash)
		if err == nil {
			return func(ctx context.Context, n chains.Network, h string, _ bool) (json.RawMessage, error) {
				return chains.VerifyTON(ctx, n, h, hint)
			}
		}
	}
	return nil
}

// Trust one operator-configured fallback for the initial lookup only. Reserve
// independently of the voting rounds so two configured endpoints can still form a pair.
func (r *NodeRegistry) firstFallback(ctx context.Context, candidates []lookupCandidate, hash string) (json.RawMessage, []lookupLease, error) {
	ctx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	_, paid, _ := r.lookup.rank(candidates)
	group := r.lookup.fill(ctx, paid.fresh, paid.cold, map[string]bool{}, 1, true)
	if len(group) == 0 {
		return nil, nil, errCapacity
	}
	node := group[0]
	read := transactionReader(r.lookup.read)
	if node.peerID != "" {
		read = r.tonProofReader(ctx, candidates, hash)
		if read == nil {
			node.release()
			return nil, group, errCapacity
		}
	}
	value, err := r.lookup.round(ctx, group, hash, func(ctx context.Context, n chains.Network, h string, public bool) (json.RawMessage, error) {
		raw, err := read(ctx, n, h, public)
		if err != nil {
			return nil, err
		}
		return chains.FallbackTransactionEvidence(n, raw)
	})
	if err != nil {
		return nil, group, err
	}
	if ctx.Err() != nil || !r.groupAllowed(ctx, group) {
		return nil, group, errCapacity
	}
	return value, group, nil
}
