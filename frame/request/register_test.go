package request

import (
	"testing"

	"github.com/kulezi/scylla-go-driver/frame"

	"github.com/google/go-cmp/cmp"
)

func TestRegister(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name     string
		content  frame.StringList
		expected []byte
	}{
		{
			name:    "Should encode and decode",
			content: frame.StringList{"TOPOLOGY_CHANGE", "STATUS_CHANGE", "SCHEMA_CHANGE"},
			expected: []byte{
				0x00, 0x03, 0x00, 0x0f, 0x54, 0x4f, 0x50, 0x4f, 0x4c, 0x4f, 0x47, 0x59, 0x5f, 0x43,
				0x48, 0x41, 0x4e, 0x47, 0x45, 0x00, 0x0d, 0x53, 0x54, 0x41, 0x54, 0x55, 0x53, 0x5f,
				0x43, 0x48, 0x41, 0x4e, 0x47, 0x45, 0x00, 0x0d, 0x53, 0x43, 0x48, 0x45, 0x4d, 0x41,
				0x5f, 0x43, 0x48, 0x41, 0x4e, 0x47, 0x45,
			},
		},
	}

	for i := 0; i < len(testCases); i++ {
		tc := testCases[i]
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var out frame.Buffer
			r := Register{tc.content}
			r.WriteTo(&out)
			if diff := cmp.Diff(out.Bytes(), tc.expected); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}
