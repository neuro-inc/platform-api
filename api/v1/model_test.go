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

	testCases := []struct {
		name, file, err string
	}{
		{
			"non-empty dataset",
			"bad.model.dataset.json",
			"field \"dataset_storage_uri\" required to be set",
		},
		{
			"non-empty result",
			"bad.model.result.json",
			"field \"result_storage_uri\" required to be set",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			src := "./testdata/fixtures/" + tc.file
			raw, err := ioutil.ReadFile(src)
			if err != nil {
				t.Fatalf("unable to read file %q: %s", src, err)
			}
			model := model{}
			err = model.UnmarshalJSON(raw)
			if err == nil {
				t.Fatalf("expected to get err")
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

	// override envPrefix for testing purpose
	envPrefix = "NP"

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
		"FOO":             "BAR",
		"NP_PATH_DATASET": "/var/storage/fixtures",
		"NP_PATH_RESULT":  "/var/storage/fixtures",
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
