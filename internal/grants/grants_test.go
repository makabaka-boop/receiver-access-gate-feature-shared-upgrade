package grants_test

// Integration tests run two real API instances (separate http.Servers with
// separate connection pools) against one PostgreSQL database, mirroring the
// multi-process deployment. Set TEST_DATABASE_URL to run them; they are
// skipped otherwise.

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"

	"grantgate/internal/grants"
)

type cluster struct {
	store *grants.Store
	api1  *httptest.Server
	api2  *httptest.Server
}

func newCluster(t *testing.T) *cluster {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL not set; skipping integration test")
	}
	open := func() *grants.Store {
		s, err := grants.NewStore(context.Background(), dsn)
		if err != nil {
			t.Fatalf("open store: %v", err)
		}
		return s
	}
	c := &cluster{store: open()}
	c.api1 = httptest.NewServer(grants.NewServer(open()))
	c.api2 = httptest.NewServer(grants.NewServer(open()))
	t.Cleanup(func() {
		c.api1.Close()
		c.api2.Close()
		c.store.Close()
	})
	return c
}

// restart simulates a full fleet restart: both API processes are torn down
// and brand-new instances come up against the same database.
func (c *cluster) restart(t *testing.T) {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	c.api1.Close()
	c.api2.Close()
	open := func() *grants.Store {
		s, err := grants.NewStore(context.Background(), dsn)
		if err != nil {
			t.Fatalf("reopen store: %v", err)
		}
		return s
	}
	s1, s2 := open(), open()
	c.api1 = httptest.NewServer(grants.NewServer(s1))
	c.api2 = httptest.NewServer(grants.NewServer(s2))
	t.Cleanup(func() {
		c.api1.Close()
		c.api2.Close()
		s1.Close()
		s2.Close()
	})
}

func receiver(t *testing.T, prefix string) string {
	t.Helper()
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return fmt.Sprintf("%s-%s", prefix, hex.EncodeToString(b))
}

type grantView struct {
	ID       string `json:"grant_id"`
	Receiver string `json:"receiver"`
	Mode     string `json:"mode"`
	Status   string `json:"status"`
}

func createGrant(t *testing.T, base, rx, mode string) (grantView, string, int) {
	t.Helper()
	body, _ := json.Marshal(map[string]string{"mode": mode})
	resp, err := http.Post(base+"/receivers/"+rx+"/grants", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusCreated {
		return grantView{}, "", resp.StatusCode
	}
	var out struct {
		grantView
		OwnerToken string `json:"owner_token"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("create decode: %v", err)
	}
	if out.OwnerToken == "" {
		t.Fatal("create response must carry owner_token")
	}
	return out.grantView, out.OwnerToken, resp.StatusCode
}

func listGrants(t *testing.T, base, rx string) []grantView {
	t.Helper()
	resp, err := http.Get(base + "/receivers/" + rx + "/grants")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("list: status %d", resp.StatusCode)
	}
	if bytes.Contains(raw, []byte("token")) {
		t.Fatalf("list response leaks token material: %s", raw)
	}
	var out struct {
		Grants []grantView `json:"grants"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("list decode: %v", err)
	}
	return out.Grants
}

func releaseGrant(t *testing.T, base, id, token string) (grantView, int) {
	t.Helper()
	body, _ := json.Marshal(map[string]string{"owner_token": token})
	resp, err := http.Post(base+"/grants/"+id+"/release", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("release: %v", err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if bytes.Contains(raw, []byte("owner_token")) {
		t.Fatalf("release response leaks token material: %s", raw)
	}
	var g grantView
	if resp.StatusCode == http.StatusOK {
		if err := json.Unmarshal(raw, &g); err != nil {
			t.Fatalf("release decode: %v", err)
		}
	}
	return g, resp.StatusCode
}

func TestConcurrentExclusiveRace(t *testing.T) {
	c := newCluster(t)
	rx := receiver(t, "rx-race")

	const contenders = 16
	codes := make(chan int, contenders)
	var wg sync.WaitGroup
	for i := 0; i < contenders; i++ {
		base := c.api1.URL
		if i%2 == 1 {
			base = c.api2.URL
		}
		wg.Add(1)
		go func(b string) {
			defer wg.Done()
			body, _ := json.Marshal(map[string]string{"mode": grants.ModeExclusive})
			resp, err := http.Post(b+"/receivers/"+rx+"/grants", "application/json", bytes.NewReader(body))
			if err != nil {
				codes <- -1
				return
			}
			defer resp.Body.Close()
			_, _ = io.Copy(io.Discard, resp.Body)
			codes <- resp.StatusCode
		}(base)
	}
	wg.Wait()
	close(codes)

	created, busy := 0, 0
	for code := range codes {
		switch code {
		case http.StatusCreated:
			created++
		case http.StatusConflict:
			busy++
		default:
			t.Fatalf("unexpected status %d", code)
		}
	}
	if created != 1 || busy != contenders-1 {
		t.Fatalf("want exactly 1 created and %d busy, got %d and %d", contenders-1, created, busy)
	}

	// The surviving grant is visible identically through both instances.
	for _, base := range []string{c.api1.URL, c.api2.URL} {
		gs := listGrants(t, base, rx)
		if len(gs) != 1 || gs[0].Mode != grants.ModeExclusive || gs[0].Status != grants.StatusActive {
			t.Fatalf("via %s: want one ACTIVE EXCLUSIVE grant, got %+v", base, gs)
		}
	}
}

func TestSharedCoexistence(t *testing.T) {
	c := newCluster(t)
	rx := receiver(t, "rx-shared")

	g1, tok1, code := createGrant(t, c.api1.URL, rx, grants.ModeShared)
	if code != http.StatusCreated {
		t.Fatalf("first SHARED: status %d", code)
	}
	g2, tok2, code := createGrant(t, c.api2.URL, rx, grants.ModeShared)
	if code != http.StatusCreated {
		t.Fatalf("second SHARED must coexist: status %d", code)
	}
	if g1.ID == g2.ID {
		t.Fatal("shared grants must have distinct ids")
	}

	if _, _, code := createGrant(t, c.api1.URL, rx, grants.ModeExclusive); code != http.StatusConflict {
		t.Fatalf("EXCLUSIVE during active SHARED: want 409, got %d", code)
	}
	if gs := listGrants(t, c.api2.URL, rx); len(gs) != 2 {
		t.Fatalf("conflict must leave no record, got %d grants", len(gs))
	}

	// Release across instances: a token minted by api1 works on api2.
	if g, code := releaseGrant(t, c.api2.URL, g1.ID, tok1); code != http.StatusOK || g.Status != grants.StatusReleased {
		t.Fatalf("release g1: code=%d status=%s", code, g.Status)
	}
	if _, _, code := createGrant(t, c.api1.URL, rx, grants.ModeExclusive); code != http.StatusConflict {
		t.Fatalf("EXCLUSIVE while g2 still active: want 409, got %d", code)
	}
	if _, code := releaseGrant(t, c.api1.URL, g2.ID, tok2); code != http.StatusOK {
		t.Fatalf("release g2: code=%d", code)
	}
	if _, _, code := createGrant(t, c.api2.URL, rx, grants.ModeExclusive); code != http.StatusCreated {
		t.Fatalf("EXCLUSIVE after all released: want 201, got %d", code)
	}

	gs := listGrants(t, c.api1.URL, rx)
	if len(gs) != 3 || gs[0].Status != grants.StatusReleased || gs[1].Status != grants.StatusReleased || gs[2].Status != grants.StatusActive {
		t.Fatalf("unexpected final state: %+v", gs)
	}
}

func TestForbiddenReleaseLeavesStateUntouched(t *testing.T) {
	c := newCluster(t)
	rx := receiver(t, "rx-forbidden")

	g, token, code := createGrant(t, c.api1.URL, rx, grants.ModeExclusive)
	if code != http.StatusCreated {
		t.Fatalf("create: status %d", code)
	}

	if _, code := releaseGrant(t, c.api2.URL, g.ID, "definitely-wrong-token"); code != http.StatusForbidden {
		t.Fatalf("wrong token: want 403, got %d", code)
	}
	gs := listGrants(t, c.api1.URL, rx)
	if len(gs) != 1 || gs[0].Status != grants.StatusActive {
		t.Fatalf("forbidden release must not change state: %+v", gs)
	}
	// The grant must still gate the receiver.
	if _, _, code := createGrant(t, c.api2.URL, rx, grants.ModeShared); code != http.StatusConflict {
		t.Fatalf("active set changed by forbidden release: want 409, got %d", code)
	}

	if _, code := releaseGrant(t, c.api1.URL, g.ID, token); code != http.StatusOK {
		t.Fatalf("correct token: want 200, got %d", code)
	}
	if gs := listGrants(t, c.api2.URL, rx); len(gs) != 1 || gs[0].Status != grants.StatusReleased {
		t.Fatalf("after release: %+v", gs)
	}
}

func TestStateAndTokensSurviveRestart(t *testing.T) {
	c := newCluster(t)
	rx := receiver(t, "rx-restart")

	g, token, code := createGrant(t, c.api1.URL, rx, grants.ModeExclusive)
	if code != http.StatusCreated {
		t.Fatalf("create: status %d", code)
	}

	c.restart(t)

	gs := listGrants(t, c.api2.URL, rx)
	if len(gs) != 1 || gs[0].ID != g.ID || gs[0].Status != grants.StatusActive {
		t.Fatalf("state lost across restart: %+v", gs)
	}
	if _, code := releaseGrant(t, c.api2.URL, g.ID, "wrong-token"); code != http.StatusForbidden {
		t.Fatalf("wrong token after restart: want 403, got %d", code)
	}
	if _, code := releaseGrant(t, c.api1.URL, g.ID, token); code != http.StatusOK {
		t.Fatalf("original token after restart: want 200, got %d", code)
	}
	if gs := listGrants(t, c.api1.URL, rx); gs[0].Status != grants.StatusReleased {
		t.Fatalf("release not persisted: %+v", gs)
	}
}
