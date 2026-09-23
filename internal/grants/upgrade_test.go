package grants_test

// Integration tests for the shared-grant upgrade feature. They run against
// the same two-instance/one-PostgreSQL cluster as the baseline tests and
// are skipped unless TEST_DATABASE_URL is set.

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"sync"
	"testing"

	"grantgate/internal/grants"
)

func upgradeGrant(t *testing.T, base, id, token string) (grantView, int) {
	t.Helper()
	body, _ := json.Marshal(map[string]string{"owner_token": token})
	resp, err := http.Post(base+"/grants/"+id+"/upgrade", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("upgrade: %v", err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if bytes.Contains(raw, []byte("owner_token")) {
		t.Fatalf("upgrade response leaks token material: %s", raw)
	}
	var g grantView
	if resp.StatusCode == http.StatusOK {
		if err := json.Unmarshal(raw, &g); err != nil {
			t.Fatalf("upgrade decode: %v", err)
		}
	}
	return g, resp.StatusCode
}

func find(t *testing.T, gs []grantView, id string) grantView {
	t.Helper()
	for _, g := range gs {
		if g.ID == id {
			return g
		}
	}
	t.Fatalf("grant %s missing from %+v", id, gs)
	return grantView{}
}

// Upgrading the sole active SHARED grant promotes it in place to ACTIVE
// EXCLUSIVE, keeping the same grant id.
func TestUpgradeImmediateExclusive(t *testing.T) {
	c := newCluster(t)
	rx := receiver(t, "rx-upg-immediate")

	g, tok, code := createGrant(t, c.api1.URL, rx, grants.ModeShared)
	if code != http.StatusCreated {
		t.Fatalf("create shared: %d", code)
	}

	ug, code := upgradeGrant(t, c.api2.URL, g.ID, tok)
	if code != http.StatusOK {
		t.Fatalf("upgrade sole shared: want 200, got %d", code)
	}
	if ug.ID != g.ID || ug.Mode != grants.ModeExclusive || ug.Status != grants.StatusActive {
		t.Fatalf("want in-place ACTIVE EXCLUSIVE, got %+v", ug)
	}

	gs := listGrants(t, c.api1.URL, rx)
	if len(gs) != 1 {
		t.Fatalf("upgrade must not create a record, got %+v", gs)
	}
	if gs[0].Mode != grants.ModeExclusive || gs[0].Status != grants.StatusActive {
		t.Fatalf("promoted grant: %+v", gs[0])
	}
	// It now behaves as a true exclusive grant.
	if _, _, code := createGrant(t, c.api2.URL, rx, grants.ModeShared); code != http.StatusConflict {
		t.Fatalf("new SHARED after promotion: want 409, got %d", code)
	}
	// The original token still releases it.
	if rg, code := releaseGrant(t, c.api1.URL, g.ID, tok); code != http.StatusOK || rg.Status != grants.StatusReleased {
		t.Fatalf("release promoted: code=%d status=%s", code, rg.Status)
	}
}

// With another active SHARED grant present, the upgrade waits as
// UPGRADE_PENDING; the receiver then refuses both new SHARED and EXCLUSIVE
// applications. When the last rival leaves, the waiter is auto-promoted
// atomically, and a query never observes an intermediate state.
func TestUpgradePendingThenAutoPromote(t *testing.T) {
	c := newCluster(t)
	rx := receiver(t, "rx-upg-pending")

	waiter, wtok, code := createGrant(t, c.api1.URL, rx, grants.ModeShared)
	if code != http.StatusCreated {
		t.Fatalf("create waiter: %d", code)
	}
	rival, rtok, code := createGrant(t, c.api2.URL, rx, grants.ModeShared)
	if code != http.StatusCreated {
		t.Fatalf("create rival: %d", code)
	}

	ug, code := upgradeGrant(t, c.api1.URL, waiter.ID, wtok)
	if code != http.StatusOK {
		t.Fatalf("upgrade with rival: want 200, got %d", code)
	}
	if ug.Status != grants.StatusUpgradePending || ug.Mode != grants.ModeShared {
		t.Fatalf("want UPGRADE_PENDING SHARED, got %+v", ug)
	}

	// Barrier: no new admissions while an upgrade is queued.
	if _, _, code := createGrant(t, c.api1.URL, rx, grants.ModeShared); code != http.StatusConflict {
		t.Fatalf("new SHARED during pending upgrade: want 409, got %d", code)
	}
	if _, _, code := createGrant(t, c.api2.URL, rx, grants.ModeExclusive); code != http.StatusConflict {
		t.Fatalf("new EXCLUSIVE during pending upgrade: want 409, got %d", code)
	}
	// Failed admissions leave no records.
	if gs := listGrants(t, c.api2.URL, rx); len(gs) != 2 {
		t.Fatalf("barred admissions must leave no record, got %+v", gs)
	}

	// The rival leaves (via the other API instance); promotion is part of
	// the same commit.
	if _, code := releaseGrant(t, c.api2.URL, rival.ID, rtok); code != http.StatusOK {
		t.Fatalf("release rival: %d", code)
	}
	for _, base := range []string{c.api1.URL, c.api2.URL} {
		gs := listGrants(t, base, rx)
		got := find(t, gs, waiter.ID)
		if got.Status != grants.StatusActive || got.Mode != grants.ModeExclusive {
			t.Fatalf("via %s: waiter must be ACTIVE EXCLUSIVE after rival leaves, got %+v", base, got)
		}
		for _, g := range gs {
			if g.Status == grants.StatusUpgradePending {
				t.Fatalf("intermediate UPGRADE_PENDING visible after promotion: %+v", gs)
			}
		}
	}
	// Original token still owns the promoted grant.
	if _, code := releaseGrant(t, c.api1.URL, waiter.ID, wtok); code != http.StatusOK {
		t.Fatalf("original token after promotion: %d", code)
	}
}

// Releasing the waiting grant itself cancels the upgrade: it is RELEASED,
// and normal admission on the receiver resumes immediately.
func TestUpgradeBarrierCancelOnSelfRelease(t *testing.T) {
	c := newCluster(t)
	rx := receiver(t, "rx-upg-cancel")

	waiter, wtok, _ := createGrant(t, c.api1.URL, rx, grants.ModeShared)
	rival, _, _ := createGrant(t, c.api2.URL, rx, grants.ModeShared)

	if g, code := upgradeGrant(t, c.api2.URL, waiter.ID, wtok); code != http.StatusOK || g.Status != grants.StatusUpgradePending {
		t.Fatalf("upgrade: code=%d g=%+v", code, g)
	}
	// Barrier active.
	if _, _, code := createGrant(t, c.api1.URL, rx, grants.ModeShared); code != http.StatusConflict {
		t.Fatalf("barrier before cancel: %d", code)
	}

	// Waiter releases itself: upgrade cancelled.
	if g, code := releaseGrant(t, c.api1.URL, waiter.ID, wtok); code != http.StatusOK || g.Status != grants.StatusReleased {
		t.Fatalf("cancel release: code=%d g=%+v", code, g)
	}

	// Rival still active, so a new EXCLUSIVE is still refused by the normal
	// rules, but a new SHARED is admitted again (barrier gone).
	if _, _, code := createGrant(t, c.api2.URL, rx, grants.ModeShared); code != http.StatusCreated {
		t.Fatalf("admission must resume after cancellation: want 201, got %d", code)
	}
	if _, _, code := createGrant(t, c.api1.URL, rx, grants.ModeExclusive); code != http.StatusConflict {
		t.Fatalf("rival still active, EXCLUSIVE must be busy: %d", code)
	}
	// The released waiter can never be upgraded afterwards.
	if _, code := upgradeGrant(t, c.api2.URL, waiter.ID, wtok); code != http.StatusConflict {
		t.Fatalf("upgrade released grant: want 409, got %d", code)
	}
	_ = rival
}

// Wrong token, released grants, native EXCLUSIVE grants and a second
// concurrent upgrade all return stable errors without changing the set.
func TestUpgradeStableErrors(t *testing.T) {
	c := newCluster(t)
	rx := receiver(t, "rx-upg-errors")

	// Unknown grant -> 404.
	if _, code := upgradeGrant(t, c.api1.URL, "does-not-exist", "tok"); code != http.StatusNotFound {
		t.Fatalf("upgrade unknown: want 404, got %d", code)
	}

	g, tok, _ := createGrant(t, c.api1.URL, rx, grants.ModeShared)
	rival, rivalTok, _ := createGrant(t, c.api2.URL, rx, grants.ModeShared)

	// Wrong token -> 403, state untouched.
	if _, code := upgradeGrant(t, c.api2.URL, g.ID, "forged-token"); code != http.StatusForbidden {
		t.Fatalf("upgrade wrong token: want 403, got %d", code)
	}
	if got := find(t, listGrants(t, c.api1.URL, rx), g.ID); got.Status != grants.StatusActive || got.Mode != grants.ModeShared {
		t.Fatalf("forbidden upgrade mutated grant: %+v", got)
	}

	// Place g as the single waiter.
	if _, code := upgradeGrant(t, c.api1.URL, g.ID, tok); code != http.StatusOK {
		t.Fatalf("make waiter: %d", code)
	}
	// A second upgrade request on the same receiver (correct token for a
	// different grant) -> 409, set unchanged.
	if _, code := upgradeGrant(t, c.api2.URL, rival.ID, rivalTok); code != http.StatusConflict {
		t.Fatalf("second upgrade: want 409, got %d", code)
	}
	if got := find(t, listGrants(t, c.api1.URL, rx), rival.ID); got.Status != grants.StatusActive {
		t.Fatalf("second upgrade mutated rival: %+v", got)
	}

	// Repeated upgrade of the same waiting grant with the correct token is
	// idempotent.
	if ug, code := upgradeGrant(t, c.api2.URL, g.ID, tok); code != http.StatusOK || ug.Status != grants.StatusUpgradePending {
		t.Fatalf("idempotent pending upgrade: code=%d g=%+v", code, ug)
	}

	// Native EXCLUSIVE grant can never be upgraded.
	rxEx := receiver(t, "rx-upg-native-ex")
	ex, extok, _ := createGrant(t, c.api1.URL, rxEx, grants.ModeExclusive)
	if _, code := upgradeGrant(t, c.api2.URL, ex.ID, extok); code != http.StatusConflict {
		t.Fatalf("upgrade native EXCLUSIVE: want 409, got %d", code)
	}
	// Repeated attempt stays a stable error (native exclusive is not an
	// idempotent promoted grant).
	if _, code := upgradeGrant(t, c.api1.URL, ex.ID, extok); code != http.StatusConflict {
		t.Fatalf("repeat native EXCLUSIVE upgrade: want 409, got %d", code)
	}
	if got := find(t, listGrants(t, c.api2.URL, rxEx), ex.ID); got.Status != grants.StatusActive || got.Mode != grants.ModeExclusive || got.ID != ex.ID {
		t.Fatalf("native exclusive mutated: %+v", got)
	}
}

// Replaying an upgrade after promotion is idempotent and reports ACTIVE
// EXCLUSIVE.
func TestUpgradeIdempotentAfterPromotion(t *testing.T) {
	c := newCluster(t)
	rx := receiver(t, "rx-upg-idempotent-ex")

	g, tok, _ := createGrant(t, c.api1.URL, rx, grants.ModeShared)
	if ug, code := upgradeGrant(t, c.api2.URL, g.ID, tok); code != http.StatusOK || ug.Mode != grants.ModeExclusive {
		t.Fatalf("first upgrade: code=%d g=%+v", code, ug)
	}
	if ug, code := upgradeGrant(t, c.api1.URL, g.ID, tok); code != http.StatusOK || ug.Mode != grants.ModeExclusive || ug.Status != grants.StatusActive || ug.ID != g.ID {
		t.Fatalf("replay after promotion must be idempotent: code=%d g=%+v", code, ug)
	}
	if _, code := upgradeGrant(t, c.api2.URL, g.ID, "wrong"); code != http.StatusForbidden {
		t.Fatalf("wrong token after promotion: want 403, got %d", code)
	}
}

// All active SHARED holders race to upgrade the same receiver: exactly one
// wins (pending or immediately exclusive) and every other request fails
// without changing the grant set.
func TestConcurrentUpgradeRace(t *testing.T) {
	c := newCluster(t)
	rx := receiver(t, "rx-upg-race")

	const n = 8
	type claim struct {
		id  string
		tok string
	}
	claims := make([]claim, n)
	for i := 0; i < n; i++ {
		base := c.api1.URL
		if i%2 == 1 {
			base = c.api2.URL
		}
		g, tok, code := createGrant(t, base, rx, grants.ModeShared)
		if code != http.StatusCreated {
			t.Fatalf("seed shared %d: %d", i, code)
		}
		claims[i] = claim{g.ID, tok}
	}

	codes := make(chan int, n)
	var wg sync.WaitGroup
	for i, cl := range claims {
		base := c.api1.URL
		if i%2 == 1 {
			base = c.api2.URL
		}
		wg.Add(1)
		go func(b, id, tok string) {
			defer wg.Done()
			_, code := upgradeGrant(t, b, id, tok)
			codes <- code
		}(base, cl.id, cl.tok)
	}
	wg.Wait()
	close(codes)

	ok, busy := 0, 0
	for code := range codes {
		switch code {
		case http.StatusOK:
			ok++
		case http.StatusConflict:
			busy++
		default:
			t.Fatalf("unexpected race status %d", code)
		}
	}
	if ok != 1 || busy != n-1 {
		t.Fatalf("want exactly 1 accepted / %d rejected, got %d / %d", n-1, ok, busy)
	}

	// Exactly one waiter exists in the committed set.
	gs := listGrants(t, c.api1.URL, rx)
	pending := 0
	for _, g := range gs {
		if g.Status == grants.StatusUpgradePending {
			pending++
		}
	}
	if pending != 1 {
		t.Fatalf("want exactly one UPGRADE_PENDING, got %d in %+v", pending, gs)
	}
}

// The queued upgrade and token validity survive a full fleet restart.
func TestUpgradePendingSurvivesRestart(t *testing.T) {
	c := newCluster(t)
	rx := receiver(t, "rx-upg-restart")

	waiter, wtok, _ := createGrant(t, c.api1.URL, rx, grants.ModeShared)
	rival, rtok, _ := createGrant(t, c.api2.URL, rx, grants.ModeShared)
	if g, code := upgradeGrant(t, c.api2.URL, waiter.ID, wtok); code != http.StatusOK || g.Status != grants.StatusUpgradePending {
		t.Fatalf("make waiter: code=%d g=%+v", code, g)
	}

	c.restart(t)

	// Pending state persists.
	got := find(t, listGrants(t, c.api1.URL, rx), waiter.ID)
	if got.Status != grants.StatusUpgradePending || got.Mode != grants.ModeShared {
		t.Fatalf("pending state lost across restart: %+v", got)
	}
	// Token still authenticates; wrong token still rejected.
	if _, code := upgradeGrant(t, c.api2.URL, waiter.ID, "wrong"); code != http.StatusForbidden {
		t.Fatalf("wrong token after restart: %d", code)
	}
	// Barrier still enforced after restart.
	if _, _, code := createGrant(t, c.api1.URL, rx, grants.ModeShared); code != http.StatusConflict {
		t.Fatalf("barrier lost across restart: %d", code)
	}
	// Rival releases after restart; auto-promotion still fires.
	if _, code := releaseGrant(t, c.api1.URL, rival.ID, rtok); code != http.StatusOK {
		t.Fatalf("release rival after restart: %d", code)
	}
	promoted := find(t, listGrants(t, c.api2.URL, rx), waiter.ID)
	if promoted.Status != grants.StatusActive || promoted.Mode != grants.ModeExclusive {
		t.Fatalf("promotion after restart failed: %+v", promoted)
	}
	if _, code := releaseGrant(t, c.api2.URL, waiter.ID, wtok); code != http.StatusOK {
		t.Fatalf("original token after restart+promotion: %d", code)
	}
}
