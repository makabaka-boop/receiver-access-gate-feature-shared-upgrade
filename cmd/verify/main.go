// Command verify runs the acceptance sequence against two live API
// processes (API1_URL, API2_URL) and exits non-zero on any failure.
package main

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

var client = &http.Client{Timeout: 10 * time.Second}

type grant struct {
	ID       string `json:"grant_id"`
	Receiver string `json:"receiver"`
	Mode     string `json:"mode"`
	Status   string `json:"status"`
}

func main() {
	api1 := strings.TrimRight(os.Getenv("API1_URL"), "/")
	api2 := strings.TrimRight(os.Getenv("API2_URL"), "/")
	if api1 == "" || api2 == "" {
		log.Fatal("API1_URL and API2_URL are required")
	}
	waitReady(api1)
	waitReady(api2)

	step("shared grants coexist across API processes")
	rx := "rx-verify-" + suffix()
	g1, tok1 := mustCreate(api1, rx, "SHARED", http.StatusCreated)
	g2, tok2 := mustCreate(api2, rx, "SHARED", http.StatusCreated)
	if g1.ID == g2.ID {
		fail("distinct shared grants must have distinct ids")
	}

	step("exclusive request conflicts with active shared grants -> 409 BUSY")
	mustCreate(api1, rx, "EXCLUSIVE", http.StatusConflict)
	grants := mustList(api2, rx)
	if len(grants) != 2 {
		fail("conflicting request must leave no record, got %d grants", len(grants))
	}

	step("release with wrong token -> 403 FORBIDDEN, record untouched")
	mustRelease(api2, g1.ID, "forged-token", http.StatusForbidden)
	if got := findGrant(mustList(api1, rx), g1.ID); got.Status != "ACTIVE" {
		fail("forbidden release mutated grant: status=%s", got.Status)
	}

	step("release with correct token -> RELEASED")
	released := mustRelease(api1, g1.ID, tok1, http.StatusOK)
	if released.Status != "RELEASED" {
		fail("expected RELEASED, got %s", released.Status)
	}

	step("exclusive still blocked while one shared grant remains active")
	mustCreate(api2, rx, "EXCLUSIVE", http.StatusConflict)

	step("exclusive succeeds once all grants are released")
	mustRelease(api1, g2.ID, tok2, http.StatusOK)
	mustCreate(api2, rx, "EXCLUSIVE", http.StatusCreated)

	step("concurrent exclusive race across both APIs yields exactly one winner")
	rx2 := "rx-verify-race-" + suffix()
	const contenders = 12
	var wg sync.WaitGroup
	results := make(chan int, contenders)
	for i := 0; i < contenders; i++ {
		base := api1
		if i%2 == 1 {
			base = api2
		}
		wg.Add(1)
		go func(b string) {
			defer wg.Done()
			_, code, _ := create(b, rx2, "EXCLUSIVE")
			results <- code
		}(base)
	}
	wg.Wait()
	close(results)
	created, busy := 0, 0
	for code := range results {
		switch code {
		case http.StatusCreated:
			created++
		case http.StatusConflict:
			busy++
		default:
			fail("unexpected race status %d", code)
		}
	}
	if created != 1 || busy != contenders-1 {
		fail("race: want 1 created / %d busy, got %d / %d", contenders-1, created, busy)
	}
	if n := len(mustList(api1, rx2)); n != 1 {
		fail("race left %d grants, want exactly 1", n)
	}

	step("query responses never leak tokens")
	body := mustListRaw(api1, rx)
	if strings.Contains(body, "token") || strings.Contains(body, tok1) {
		fail("list response leaks token material: %s", body)
	}

	fmt.Println("VERIFY OK")
}

func step(format string, args ...any) { log.Printf("STEP: "+format, args...) }

func fail(format string, args ...any) { log.Fatalf("FAIL: "+format, args...) }

func suffix() string {
	b := make([]byte, 6)
	if _, err := rand.Read(b); err != nil {
		log.Fatal(err)
	}
	return hex.EncodeToString(b)
}

func waitReady(base string) {
	deadline := time.Now().Add(90 * time.Second)
	for {
		resp, err := client.Get(base + "/healthz")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		if time.Now().After(deadline) {
			fail("api at %s not ready", base)
		}
		time.Sleep(time.Second)
	}
}

func create(base, receiver, mode string) (grant, int, string) {
	payload, _ := json.Marshal(map[string]string{"mode": mode})
	resp, err := client.Post(base+"/receivers/"+receiver+"/grants", "application/json", bytes.NewReader(payload))
	if err != nil {
		fail("create: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusCreated {
		return grant{}, resp.StatusCode, ""
	}
	var out struct {
		grant
		OwnerToken string `json:"owner_token"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		fail("create: decode: %v", err)
	}
	if out.OwnerToken == "" {
		fail("create response must carry owner_token exactly once")
	}
	return out.grant, resp.StatusCode, out.OwnerToken
}

func mustCreate(base, receiver, mode string, want int) (grant, string) {
	g, code, token := create(base, receiver, mode)
	if code != want {
		fail("create %s on %s: want %d, got %d", mode, receiver, want, code)
	}
	return g, token
}

func mustList(base, receiver string) []grant {
	body := mustListRaw(base, receiver)
	var out struct {
		Grants []grant `json:"grants"`
	}
	if err := json.Unmarshal([]byte(body), &out); err != nil {
		fail("list: decode: %v", err)
	}
	return out.Grants
}

func mustListRaw(base, receiver string) string {
	resp, err := client.Get(base + "/receivers/" + receiver + "/grants")
	if err != nil {
		fail("list: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		fail("list: want 200, got %d", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	return string(body)
}

func findGrant(grants []grant, id string) grant {
	for _, g := range grants {
		if g.ID == id {
			return g
		}
	}
	fail("grant %s missing from list", id)
	return grant{}
}

func mustRelease(base, id, token string, want int) grant {
	payload, _ := json.Marshal(map[string]string{"owner_token": token})
	resp, err := client.Post(base+"/grants/"+id+"/release", "application/json", bytes.NewReader(payload))
	if err != nil {
		fail("release: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != want {
		fail("release %s: want %d, got %d (%s)", id, want, resp.StatusCode, body)
	}
	if strings.Contains(string(body), "owner_token") {
		fail("release response leaks owner_token")
	}
	if want != http.StatusOK {
		return grant{}
	}
	var g grant
	if err := json.Unmarshal(body, &g); err != nil {
		fail("release: decode: %v", err)
	}
	return g
}
