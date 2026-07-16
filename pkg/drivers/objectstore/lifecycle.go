// Package objectstore holds shared parameter definitions for the
// object-store driver family (gcs, s3) per SPEC §Driver families:
// property definitions written once so a portable Buckety CR
// carrying only family-common parameters validates and provisions
// against any bucket driver (issue #17). Drivers translate the
// neutral shapes here into their backend's native API; a family
// parameter a backend cannot express fully is handled per the
// family's fail-safe rules documented on each driver.
//
// The family's common parameters:
//
//	versioning  "true" | "false"
//	lifecycle   JSON in the gsutil `lifecycle set` shape
//
// kadm shares nothing with this family and is deliberately not in
// it - families are per-service-kind, not one flat schema.
package objectstore

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// Rule is a parsed lifecycle rule in neutral form.
type Rule struct {
	Action    Action
	Condition Condition
}

// Action is what happens when a rule's conditions match.
type Action struct {
	// Type is one of Delete, SetStorageClass,
	// AbortIncompleteMultipartUpload.
	Type string
	// StorageClass accompanies SetStorageClass.
	StorageClass string
}

// Condition gates a rule. Zero values mean "not set" except for
// the pointer fields, where nil distinguishes unset from zero.
type Condition struct {
	Age                     *int64
	CreatedBefore           time.Time
	CustomTimeBefore        time.Time
	NoncurrentTimeBefore    time.Time
	DaysSinceCustomTime     int64
	DaysSinceNoncurrentTime int64
	IsLive                  *bool
	MatchesPrefix           []string
	MatchesStorageClass     []string
	MatchesSuffix           []string
	NumNewerVersions        int64
}

// The wire shape is the same document `gsutil lifecycle set`
// takes, so operators can move an existing policy file into the
// Buckety verbatim:
//
//	{"rule": [{"action": {"type": "Delete"}, "condition": {"age": 30}}]}
//
// Decoding is strict: unknown action types and condition fields
// are admission errors, not silent drops.
type lifecycleDoc struct {
	Rule []lifecycleRule `json:"rule"`
}

type lifecycleRule struct {
	Action    lifecycleAction    `json:"action"`
	Condition lifecycleCondition `json:"condition"`
}

type lifecycleAction struct {
	Type         string `json:"type"`
	StorageClass string `json:"storageClass,omitempty"`
}

type lifecycleCondition struct {
	Age                     *int64   `json:"age,omitempty"`
	CreatedBefore           string   `json:"createdBefore,omitempty"`
	CustomTimeBefore        string   `json:"customTimeBefore,omitempty"`
	DaysSinceCustomTime     int64    `json:"daysSinceCustomTime,omitempty"`
	DaysSinceNoncurrentTime int64    `json:"daysSinceNoncurrentTime,omitempty"`
	IsLive                  *bool    `json:"isLive,omitempty"`
	MatchesPrefix           []string `json:"matchesPrefix,omitempty"`
	MatchesStorageClass     []string `json:"matchesStorageClass,omitempty"`
	MatchesSuffix           []string `json:"matchesSuffix,omitempty"`
	NoncurrentTimeBefore    string   `json:"noncurrentTimeBefore,omitempty"`
	NumNewerVersions        int64    `json:"numNewerVersions,omitempty"`
}

// ParseLifecycle decodes and validates the family's lifecycle
// parameter. An empty rule list is valid and means "clear all
// rules" (managed-empty), distinct from omitting the parameter
// (unmanaged).
func ParseLifecycle(doc string) ([]Rule, error) {
	dec := json.NewDecoder(strings.NewReader(doc))
	dec.DisallowUnknownFields()
	var parsed lifecycleDoc
	if err := dec.Decode(&parsed); err != nil {
		return nil, fmt.Errorf("not a valid gsutil lifecycle document: %w", err)
	}
	if parsed.Rule == nil {
		return nil, fmt.Errorf("not a valid gsutil lifecycle document: missing \"rule\" list (use {\"rule\": []} to clear all rules)")
	}
	out := make([]Rule, 0, len(parsed.Rule))
	for i, r := range parsed.Rule {
		rule := Rule{}
		switch r.Action.Type {
		case "Delete", "AbortIncompleteMultipartUpload":
			if r.Action.StorageClass != "" {
				return nil, fmt.Errorf("rule[%d].action.storageClass is only valid with type SetStorageClass", i)
			}
			rule.Action.Type = r.Action.Type
		case "SetStorageClass":
			if r.Action.StorageClass == "" {
				return nil, fmt.Errorf("rule[%d].action.storageClass is required with type SetStorageClass", i)
			}
			rule.Action = Action{Type: r.Action.Type, StorageClass: r.Action.StorageClass}
		case "":
			return nil, fmt.Errorf("rule[%d].action.type is required", i)
		default:
			return nil, fmt.Errorf("rule[%d].action.type %q is not one of Delete, SetStorageClass, AbortIncompleteMultipartUpload", i, r.Action.Type)
		}
		c := r.Condition
		rule.Condition.Age = c.Age
		var err error
		if rule.Condition.CreatedBefore, err = parseDate(c.CreatedBefore, i, "createdBefore"); err != nil {
			return nil, err
		}
		if rule.Condition.CustomTimeBefore, err = parseDate(c.CustomTimeBefore, i, "customTimeBefore"); err != nil {
			return nil, err
		}
		if rule.Condition.NoncurrentTimeBefore, err = parseDate(c.NoncurrentTimeBefore, i, "noncurrentTimeBefore"); err != nil {
			return nil, err
		}
		rule.Condition.DaysSinceCustomTime = c.DaysSinceCustomTime
		rule.Condition.DaysSinceNoncurrentTime = c.DaysSinceNoncurrentTime
		rule.Condition.IsLive = c.IsLive
		rule.Condition.MatchesPrefix = c.MatchesPrefix
		rule.Condition.MatchesStorageClass = c.MatchesStorageClass
		rule.Condition.MatchesSuffix = c.MatchesSuffix
		rule.Condition.NumNewerVersions = c.NumNewerVersions
		out = append(out, rule)
	}
	return out, nil
}

func parseDate(v string, i int, field string) (time.Time, error) {
	if v == "" {
		return time.Time{}, nil
	}
	t, err := time.Parse("2006-01-02", v)
	if err != nil {
		return time.Time{}, fmt.Errorf("rule[%d].condition.%s: want YYYY-MM-DD, got %q", i, field, v)
	}
	return t, nil
}
