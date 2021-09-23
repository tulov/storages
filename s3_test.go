package storages

import (
	"github.com/stretchr/testify/assert"
	"github.com/ulule/gostorages"
	"os"
	"testing"
	"time"
)

func Test(t *testing.T) {
	accessKeyID := os.Getenv("AWS_ACCESS_KEY_ID")
	secretAccessKey := os.Getenv("AWS_SECRET_ACCESS_KEY")
	region := os.Getenv("AWS_REGION")
	bucket := os.Getenv("S3_BUCKET")
	endpointUrl := os.Getenv("AWS_ENDPOINT_URL")
	baseURL := os.Getenv("PICFIT_BASE_URL")
	location := os.Getenv("PICFIT_LOCATION")
	if accessKeyID == "" ||
		secretAccessKey == "" ||
		region == "" ||
		bucket == "" {
		t.SkipNow()
	}
	storage := NewS3Storage(endpointUrl)
	filename := "test.txt"
	txt := []byte("a content example")
	before := time.Now().Add(time.Second * time.Duration(-1))
	err := storage.Save(filename, gostorages.NewContentFile(txt))
	after := time.Now().Add(time.Second)
	if err != nil {
		t.Fatal(err)
	}

	assert.True(t, storage.Exists(filename))

	storageFile, err := storage.Open(filename)

	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, int64(len(txt)), storageFile.Size())

	assert.Equal(t, storage.URL("test"), baseURL+"/"+location+"/test")

	modified, err := storage.ModifiedTime(filename)

	if err != nil {
		t.Fatal(err)
	}
	if modified.Before(before) {
		t.Errorf("expected modtime to be after %v, got %v", before, modified)
	}
	if modified.After(after) {
		t.Errorf("expected modtime to be before %v, got %v", after, modified)
	}

	err = storage.Delete(filename)

	if err != nil {
		t.Fatal(err)
	}
}
