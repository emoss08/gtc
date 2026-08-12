package doctor

import (
	"strings"
	"testing"
)

func TestURLRewriting(t *testing.T) {
	replicated := "postgres://user:pw@db.example.com:5432/app?replication=database&sslmode=require"

	plain := plainURL(replicated)
	if strings.Contains(plain, "replication=") {
		t.Errorf("plainURL kept the replication param: %s", plain)
	}
	if !strings.Contains(plain, "sslmode=require") {
		t.Errorf("plainURL dropped unrelated params: %s", plain)
	}

	rep := replicationURL("postgres://user:pw@db.example.com:5432/app?sslmode=require")
	if !strings.Contains(rep, "replication=database") {
		t.Errorf("replicationURL did not add the replication param: %s", rep)
	}

	// Unparsable input passes through so the connect attempt reports it.
	if got := plainURL("::not-a-url"); got != "::not-a-url" {
		t.Errorf("plainURL mangled unparsable input: %s", got)
	}
}

func TestReportExitCode(t *testing.T) {
	var sb strings.Builder
	r := &report{out: &sb}
	r.add("a", pass, "fine")
	r.add("b", warn, "meh")
	r.add("c", skip, "not configured")

	warns, fails := 0, 0
	for _, res := range r.results {
		switch res.status {
		case warn:
			warns++
		case fail:
			fails++
		}
	}
	if warns != 1 || fails != 0 {
		t.Errorf("warns=%d fails=%d, want 1/0", warns, fails)
	}

	out := sb.String()
	for _, want := range []string{"✓ a", "! b", "- c"} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q:\n%s", want, out)
		}
	}
}
