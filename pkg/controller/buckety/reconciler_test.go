package buckety

import "testing"

func TestMajorOf(t *testing.T) {
	cases := []struct {
		in      string
		want    int
		wantErr bool
	}{
		{"0.1.0", 0, false},
		{"1.0.0", 1, false},
		{"12.3.4", 12, false},
		{"7", 7, false},
		{"", 0, true},
		{"v1.0.0", 0, true},
		{"one.two", 0, true},
	}
	for _, c := range cases {
		got, err := majorOf(c.in)
		if c.wantErr {
			if err == nil {
				t.Errorf("majorOf(%q): expected error, got %d", c.in, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("majorOf(%q): %v", c.in, err)
			continue
		}
		if got != c.want {
			t.Errorf("majorOf(%q) = %d, want %d", c.in, got, c.want)
		}
	}
}
