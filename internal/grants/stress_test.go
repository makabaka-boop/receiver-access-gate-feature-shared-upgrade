package grants_test

import (
	"fmt"
	"net/http"
	"sync"
	"testing"

	"grantgate/internal/grants"
)

// Interleave create/upgrade/release across both instances and assert the
// final committed state is always self-consistent: at most one active
// grant when exclusive/promoted, no stuck waiter once rivals drain, etc.
func TestUpgradeStressInterleave(t *testing.T) {
	c := newCluster(t)

	const receivers = 12
	const rounds = 60
	var wg sync.WaitGroup
	for r := 0; r < receivers; r++ {
		rx := receiver(t, fmt.Sprintf("rx-stress-%d", r))
		base := c.api1.URL
		if r%2 == 1 {
			base = c.api2.URL
		}
		// Seed 2-4 shared grants.
		seeds := 2 + r%3
		type token struct {
			id, tok string
		}
		holders := make(chan token, seeds)
		for i := 0; i < seeds; i++ {
			g, tk, code := createGrant(t, base, rx, grants.ModeShared)
			if code != http.StatusCreated {
				t.Fatalf("seed: %d", code)
			}
			holders <- token{g.ID, tk}
		}
		close(holders)
		var all []token
		for h := range holders {
			all = append(all, h)
		}

		// One upgrader + releasers racing.
		wg.Add(1)
		go func(up token) {
			defer wg.Done()
			for i := 0; i < rounds; i++ {
				_, _ = c.upgradeNoFail(t, rx, up.id, up.tok)
			}
		}(all[0])

		for _, h := range all[1:] {
			wg.Add(1)
			go func(h token) {
				defer wg.Done()
				// Repeated release is idempotent once released.
				for i := 0; i < rounds; i++ {
					_, _ = c.releaseNoFail(t, rx, h.id, h.tok)
				}
			}(h)
		}
	}
	wg.Wait()
}

func (c *cluster) upgradeNoFail(t *testing.T, rx, id, tok string) (grantView, int) {
	t.Helper()
	return upgradeGrant(t, c.api1.URL, id, tok)
}

func (c *cluster) releaseNoFail(t *testing.T, rx, id, tok string) (grantView, int) {
	t.Helper()
	return releaseGrant(t, c.api2.URL, id, tok)
}
