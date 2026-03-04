package courtview

import "testing"

func TestNormalizeCaseNumber(t *testing.T) {
	tests := []struct {
		in      string
		want    string
		wantErr bool
	}{
		{in: "3AN-11-00123CR", want: "3AN-11-00123CR"},
		{in: "3ANS11123CR", want: "3AN-11-00123CR"},
		{in: "3AN1100123", want: "3AN-11-00123CR"},
		{in: "3ANX1100123CR", want: "3AN-11-00123CR"},
		{in: "3AN12CR", want: "3AN-12-00000CR"},
		{in: "3KE-25-184CR", want: "3KE-25-00184CR"},
		{in: "1KE-25-00184CR", want: "1KE-25-00184CR"},
		{in: "an11123cr", wantErr: true},
		{in: "3AN12345678CR", wantErr: true},
	}

	for _, tc := range tests {
		got, err := NormalizeCaseNumber(tc.in)
		if tc.wantErr {
			if err == nil {
				t.Fatalf("NormalizeCaseNumber(%q): expected error, got %q", tc.in, got)
			}
			continue
		}
		if err != nil {
			t.Fatalf("NormalizeCaseNumber(%q): unexpected error: %v", tc.in, err)
		}
		if got != tc.want {
			t.Fatalf("NormalizeCaseNumber(%q): got %q, want %q", tc.in, got, tc.want)
		}
	}
}
