package v1

import (
	"os"
	"strings"
	"testing"

	"github.com/neuromation/platform-api/api/v1/config"
	"github.com/neuromation/platform-api/log"
)

const testDir = "./testdata/temp"

func TestMain(m *testing.M) {
	if err := os.MkdirAll(testDir, 0700); err != nil {
		log.Fatalf("unable to create dir %q: %s", testDir, err)
	}
	log.SuppressOutput(true)
	retCode := m.Run()
	log.SuppressOutput(false)
	if err := os.RemoveAll(testDir); err != nil {
		log.Fatalf("cannot remove %q: %s", testDir, err)
	}
	os.Exit(retCode)
}

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
			"bad storage path",
			"error while initing storage",
			&config.Config{
				ListenAddr: ":8080",
			},
		},
		{
			"bad client addr",
			"error while creating client",
			&config.Config{
				SingularityAddr: "",
				StorageBasePath: testDir,
			},
		},
		{
			"ping error",
			"error while establishing connection",
			&config.Config{
				SingularityAddr: "https://127.0.0.1:9999",
				StorageBasePath: testDir,
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
