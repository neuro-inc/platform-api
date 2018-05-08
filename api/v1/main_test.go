package v1

import (
	"strings"
	"testing"

	"github.com/neuromation/platform-api/api/v1/config"
)

func TestServe_Negative(t *testing.T) {
	var testCases = []struct {
		name, expErr string
		cfg          *config.Config
	}{
		{
			"bad listen addr",
			"cannot listen for",
			&config.Config{
				ListenAddr: "foo",
			},
		},
		{
			"bad client addr",
			"error while creating client",
			&config.Config{
				SingularityAddr: "",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := Serve(tc.cfg)
			if err == nil {
				t.Fatalf("expected to get an error; got nil")
			}

			if !strings.Contains(err.Error(), tc.expErr) {
				t.Fatalf("expected to have err %q; got %q", tc.expErr, err)
			}
		})
	}
}
