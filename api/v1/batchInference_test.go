package v1

import (
	"fmt"
	"io/ioutil"
	"testing"

	"github.com/neuromation/platform-api/api/v1/storage"
)

func TestBatchInference_UnmarshalJSON_Negative(t *testing.T) {
	if err := storage.Init("./testdata"); err != nil {
		t.Fatalf("error while initing storage: %s", err)
	}

	testCases := []struct {
		name, file, field string
	}{
		{
			"non-empty container",
			"bad.batch.container.json",
			"container",
		},
		{
			"non-empty resources",
			"bad.batch.resources.json",
			"resources",
		},
		{
			"non-empty dataset",
			"bad.batch.inference.dataset.json",
			"dataset_storage_uri",
		},
		{
			"non-empty result",
			"bad.batch.inference.result.json",
			"result_storage_uri",
		},
		{
			"non-empty model",
			"bad.batch.inference.model.json",
			"model_storage_uri",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			src := "./testdata/fixtures/" + tc.file
			raw, err := ioutil.ReadFile(src)
			if err != nil {
				t.Fatalf("unable to read file %q: %s", src, err)
			}
			bi := batchInference{}
			err = bi.UnmarshalJSON(raw)
			if err == nil {
				t.Fatalf("expected to get err")
			}
			expErr := fmt.Sprintf("field %q required to be set", tc.field)
			if err.Error() != expErr {
				t.Fatalf("expected to get err: %q; got instead: %q", expErr, err)
			}
		})
	}
}

func TestBatchInference_UnmarshalJSON_Positive(t *testing.T) {
	if err := storage.Init("./testdata"); err != nil {
		t.Fatalf("error while initing storage: %s", err)
	}

	// override envPrefix for testing purpose
	envPrefix = "NP"

	goodSrc := "./testdata/fixtures/good.batch.inference.json"
	raw, err := ioutil.ReadFile(goodSrc)
	if err != nil {
		t.Fatalf("unable to read file %q: %s", goodSrc, err)
	}
	bi := batchInference{}
	err = bi.UnmarshalJSON(raw)
	if err != nil {
		t.Fatalf("unexpected err: %s", err)
	}

	if bi.Container.Image != "hello-world" {
		t.Fatalf("wrong image %q; expected to have %q", bi.Container.Image, "hello-world")
	}

	expCMD := "ls -la"
	if bi.Container.CMD != expCMD {
		t.Fatalf("wrong CMD %q; expected to have %q", bi.Container.CMD, expCMD)
	}

	expectedEnv := map[string]string{
		"FOO":             "BAR",
		"NP_PATH_DATASET": "/var/storage/fixtures",
		"NP_PATH_RESULT":  "/var/storage/fixtures",
		"NP_PATH_MODEL":   "/var/storage/fixtures",
	}
	for expK, expV := range expectedEnv {
		v, ok := bi.Container.Env[expK]
		if !ok {
			t.Fatalf("key %q is absent in container variables", expK)
		}
		if v != expV {
			t.Fatalf("got key %q value %q; expected to be %q", expK, v, expV)
		}
	}
	if bi.DatasetStorageURI.Mode != "RO" {
		t.Fatalf("wrong DatasetStorageURI mode %s; expexted to be RO", bi.DatasetStorageURI.Mode)
	}
	if bi.ModelStorageURI.Mode != "RO" {
		t.Fatalf("wrong ModelStorageURI mode %s; expexted to be RO", bi.ModelStorageURI.Mode)
	}
	if bi.ResultStorageURI.Mode != "RW" {
		t.Fatalf("wrong ResultStorageURI mode %s; expexted to be RW", bi.ResultStorageURI.Mode)
	}
}
