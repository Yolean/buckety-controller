package mysql

import (
	"crypto/rand"
	"errors"
	"fmt"
	"math/big"
	"regexp"
	"strings"

	gomysql "github.com/go-sql-driver/mysql"
)

// Passwords are drawn from the URL-unreserved characters: they need
// no escaping in a SQL string literal, a mysql:// URL, a shell
// heredoc or an env var. Generated ones contain all four character
// classes so password-policy plugins (simple_password_check) accept
// them.
const (
	passwordLen      = 32
	minPasswordLen   = 16
	maxPasswordLen   = 128
	passwordSpecials = "-._~"
	passwordAlphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789" + passwordSpecials
)

var passwordRE = regexp.MustCompile(`^[A-Za-z0-9._~-]+$`)

func generatePassword() (string, error) {
	max := big.NewInt(int64(len(passwordAlphabet)))
	b := make([]byte, passwordLen)
	for {
		for i := range b {
			n, err := rand.Int(rand.Reader, max)
			if err != nil {
				return "", fmt.Errorf("mysql: generate password: %w", err)
			}
			b[i] = passwordAlphabet[n.Int64()]
		}
		if hasAllClasses(string(b)) {
			return string(b), nil
		}
	}
}

func hasAllClasses(s string) bool {
	return strings.ContainsAny(s, "ABCDEFGHIJKLMNOPQRSTUVWXYZ") &&
		strings.ContainsAny(s, "abcdefghijklmnopqrstuvwxyz") &&
		strings.ContainsAny(s, "0123456789") &&
		strings.ContainsAny(s, passwordSpecials)
}

// validatePassword checks a password taken from an access Secret.
// The error never contains the value.
func validatePassword(p string) error {
	if len(p) < minPasswordLen || len(p) > maxPasswordLen || !passwordRE.MatchString(p) {
		return fmt.Errorf("password must be %d-%d characters of A-Z a-z 0-9 and %s", minPasswordLen, maxPasswordLen, passwordSpecials)
	}
	return nil
}

// redact removes secret from an error's text. Server errors can
// quote the statement, and the statement can carry a password.
func redact(err error, secret string) error {
	if err == nil || secret == "" || !strings.Contains(err.Error(), secret) {
		return err
	}
	return redactedError(strings.ReplaceAll(err.Error(), secret, "<redacted>"))
}

// statementError describes an error from a statement that carried a
// password. A server error keeps only its number: messages such as
// a parse error's "near '...'" quote the statement and may cut it
// mid-password, which redact cannot catch.
func statementError(err error, password string) error {
	var me *gomysql.MySQLError
	if errors.As(err, &me) {
		hint := ""
		switch me.Number {
		case 1819:
			hint = ": the server's password policy rejected the password"
		case 1227:
			hint = ": the controller's account lacks the CREATE USER privilege"
		}
		return redactedError(fmt.Sprintf("server error %d%s", me.Number, hint))
	}
	return redact(err, password)
}

type redactedError string

func (e redactedError) Error() string { return string(e) }
