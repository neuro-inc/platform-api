package container

import (
	"encoding/json"
	"io/ioutil"
	"strings"
	"testing"

	"github.com/neuromation/platform-api/api/v1/storage"
)

func TestUnmarshalJSON_Negative(t *testing.T) {
	if err := storage.Init("./testdata"); err != nil {
		t.Fatalf("error while initing storage: %s", err)
	}

	var testCases = []struct {
		name string
		file string
		err  string
	}{
		{
			"corrupted json",
			"./testdata/fixtures/bad.corrupted.json",
			"unexpected end of JSON input",
		},
		{
			"bad dataset path",
			"./testdata/fixtures/bad.dataset.json",
			"invalid path",
		},
		{
			"bad volume RO path",
			"./testdata/fixtures/bad.volume.ro.json",
			"invalid path",
		},
		{
			"bad volume RW path",
			"./testdata/fixtures/bad.volume.rw.json",
			"invalid path",
		},
		{
			"no cpus resource",
			"./testdata/fixtures/bad.resource.json",
			"cpus param must be set",
		},
	}

	type job struct {
		Container Container `json:"container"`
		Resources Resources `json:"resources"`
		RO        VolumeRO  `json:"volumeRO"`
		RW        VolumeRW  `json:"volumeRW"`
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			raw, err := ioutil.ReadFile(tc.file)
			if err != nil {
				t.Fatalf("unable to read file %q: %s", tc.file, err)
			}
			j := &job{}
			err = json.Unmarshal(raw, j)
			if err == nil {
				t.Fatalf("expected to get error; got nil")
			}
			if !strings.Contains(err.Error(), tc.err) {
				t.Fatalf("expected to get err: %s; got instead: %q", tc.err, err)
			}
		})
	}
}
