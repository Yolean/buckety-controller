package objectstore

import "testing"

// Family-level parse rules; driver-specific translation limits are
// tested in each driver's package.
func TestParseLifecycle(t *testing.T) {
	rules, err := ParseLifecycle(`{"rule": [
	  {"action": {"type": "Delete"}, "condition": {"age": 7, "matchesPrefix": ["board-prints/"]}},
	  {"action": {"type": "Delete"}, "condition": {"age": 1, "matchesPrefix": [".staging/"]}}
	]}`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(rules) != 2 || *rules[0].Condition.Age != 7 || rules[1].Condition.MatchesPrefix[0] != ".staging/" {
		t.Errorf("rules: %+v", rules)
	}

	if empty, err := ParseLifecycle(`{"rule": []}`); err != nil || len(empty) != 0 {
		t.Errorf("managed-empty: rules=%v err=%v", empty, err)
	}

	for name, doc := range map[string]string{
		"not json":       "nope",
		"no rule key":    `{}`,
		"unknown action": `{"rule": [{"action": {"type": "Recycle"}, "condition": {}}]}`,
		"unknown cond":   `{"rule": [{"action": {"type": "Delete"}, "condition": {"aeg": 1}}]}`,
		"class w/o type": `{"rule": [{"action": {"type": "SetStorageClass"}, "condition": {}}]}`,
		"bad date":       `{"rule": [{"action": {"type": "Delete"}, "condition": {"createdBefore": "soon"}}]}`,
	} {
		if _, err := ParseLifecycle(doc); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}
