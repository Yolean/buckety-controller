package mysql

import (
	"errors"
	"strings"
	"testing"
)

func TestGeneratePassword(t *testing.T) {
	seen := map[string]bool{}
	for i := 0; i < 200; i++ {
		p, err := generatePassword()
		if err != nil {
			t.Fatal(err)
		}
		if len(p) != passwordLen || !passwordRE.MatchString(p) || !hasAllClasses(p) {
			t.Fatalf("generated password %d does not meet the policy", i)
		}
		if err := validatePassword(p); err != nil {
			t.Fatalf("generated password rejected: %v", err)
		}
		if seen[p] {
			t.Fatal("generated password repeated")
		}
		seen[p] = true
	}
}

func TestValidatePassword(t *testing.T) {
	if err := validatePassword("Abcdefgh-1234567"); err != nil {
		t.Errorf("16 safe characters: %v", err)
	}
	bad := []string{
		"", "short-1A", strings.Repeat("a", 129),
		"Abcdefgh-123456'", `Abcdefgh-123456\`, "Abcdefgh-123456 ", "Abcdefgh-123456@",
		"Abcdefgh-123456;", "Abcdefgh-123456`", "Abcdefgh-123456ä", "Abcdefgh-123456\n",
	}
	for _, p := range bad {
		err := validatePassword(p)
		if err == nil {
			t.Errorf("password %d accepted", len(p))
			continue
		}
		if p != "" && strings.Contains(err.Error(), p) {
			t.Errorf("validation error echoes the password")
		}
	}
}

func TestRedact(t *testing.T) {
	err := redact(errors.New("near 'Secret-Value-123' at line 1"), "Secret-Value-123")
	if strings.Contains(err.Error(), "Secret-Value-123") || !strings.Contains(err.Error(), "<redacted>") {
		t.Fatalf("redact = %v", err)
	}
	orig := errors.New("unrelated")
	if redact(orig, "x-y-z") != orig {
		t.Fatal("redact replaced an error without the secret")
	}
	if redact(nil, "x") != nil {
		t.Fatal("redact(nil)")
	}
}
