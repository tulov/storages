package s3

import (
	"bytes"
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/tulov/storages"
)

func Test(t *testing.T) {
	accessKeyID := os.Getenv("AWS_ACCESS_KEY_ID")
	secretAccessKey := os.Getenv("AWS_SECRET_ACCESS_KEY")
	region := os.Getenv("AWS_REGION")
	bucket := os.Getenv("S3_BUCKET")
	endpointUrl := os.Getenv("AWS_ENDPOINT_URL")
	if accessKeyID == "" ||
		secretAccessKey == "" ||
		region == "" ||
		bucket == "" {
		t.SkipNow()
	}
	storage, err := NewStorage(Config{
		Bucket:      bucket,
		EndpointUrl: endpointUrl,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	if _, err = storage.Stat(ctx, "doesnotexist"); !errors.Is(err, storages.ErrNotExist) {
		t.Errorf("expected not exists, got %v", err)
	}

	before := time.Now().Add(time.Second * time.Duration(-1))
	if err := storage.Save(ctx, bytes.NewBufferString("hello"), "world"); err != nil {
		t.Fatal(err)
	}
	now := time.Now().Add(time.Second)

	stat, err := storage.Stat(ctx, "world")
	if err != nil {
		t.Errorf("unexpected error %v", err)
	}
	if stat.Size != 5 {
		t.Errorf("expected size to be %d, got %d", 5, stat.Size)
	}
	if stat.ModifiedTime.Before(before) {
		t.Errorf("expected modtime to be after %v, got %v", before, stat.ModifiedTime)
	}
	if stat.ModifiedTime.After(now) {
		t.Errorf("expected modtime to be before %v, got %v", now, stat.ModifiedTime)
	}
}
