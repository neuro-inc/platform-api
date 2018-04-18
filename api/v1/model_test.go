package v1

import (
	"io/ioutil"
	"strings"
	"testing"

	"github.com/neuromation/platform-api/api/v1/storage"
)

func TestModel_UnmarshalJSON_Negative(t *testing.T) {
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
			"./testdata/fixtures/bad.model.corrupted.json",
			"unexpected end of JSON input",
		},
		{
			"bad dataset path",
			"./testdata/fixtures/bad.model.dataset.json",
			"invalid path",
		},
		{
			"bad result path",
			"./testdata/fixtures/bad.model.result.json",
			"invalid path",
		},
		{
			"no cpus resource",
			"./testdata/fixtures/bad.model.resource.json",
			"cpus param must be set",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			raw, err := ioutil.ReadFile(tc.file)
			if err != nil {
				t.Fatalf("unable to read file %q: %s", tc.file, err)
			}
			model := model{}
			err = model.UnmarshalJSON(raw)
			if err == nil {
				t.Fatalf("expected to get error; got nil")
			}
			if !strings.Contains(err.Error(), tc.err) {
				t.Fatalf("expected to get err: %s; got instead: %q", tc.err, err)
			}
		})
	}
}

func TestModel_UnmarshalJSON_Positive(t *testing.T) {
	if err := storage.Init("./testdata"); err != nil {
		t.Fatalf("error while initing storage: %s", err)
	}

	goodSrc := "./testdata/fixtures/good.model.json"
	raw, err := ioutil.ReadFile(goodSrc)
	if err != nil {
		t.Fatalf("unable to read file %q: %s", goodSrc, err)
	}
	model := model{}
	err = model.UnmarshalJSON(raw)
	if err != nil {
		t.Fatalf("unexpected err: %s", err)
	}

	if model.Container.Image != "hello-world" {
		t.Fatalf("wrong image %q; expected to have %q", model.Container.Image, "hello-world")
	}

	expectedEnv := map[string]string{
		"FOO":          "BAR",
		"PATH_DATASET": "/var/storage/fixtures",
		"PATH_RESULT":  "/var/storage/fixtures",
	}
	for expK, expV := range expectedEnv {
		v, ok := model.Container.Env[expK]
		if !ok {
			t.Fatalf("key %q is absent in container variables", expK)
		}
		if v != expV {
			t.Fatalf("got key %q value %q; expected to be %q", expK, v, expV)
		}
	}
	if model.DatasetStorageURI.Mode != "RO" {
		t.Fatalf("wrong DatasetStorageURI mode %s; expexted to be RO", model.DatasetStorageURI.Mode)
	}
	if model.ResultStorageURI.Mode != "RW" {
		t.Fatalf("wrong ResultStorageURI mode %s; expexted to be RW", model.ResultStorageURI.Mode)
	}
}
