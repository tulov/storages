package storages

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	"github.com/aws/smithy-go/transport/http"
	ss3 "github.com/mitchellh/goamz/s3"
	"github.com/ulule/gostorages"
	"log"
	"os"
	"time"
)

// Storage is a S3 storage.
type S3Storage struct {
	*gostorages.BaseStorage
	bucket string
	s3     *s3.Client
}

type stat struct {
	Size         int64
	ModifiedTime time.Time
}

func NewS3Storage(endpointUrl string) *S3Storage {
	envKeys := []string{
		"AWS_ACCESS_KEY_ID",
		"AWS_SECRET_ACCESS_KEY",
		"AWS_REGION",
		"S3_BUCKET",
		"PICFIT_BASE_URL",
		"PICFIT_LOCATION",
	}
	for _, s := range envKeys {
		tmp := os.Getenv(s)
		if tmp == "" {
			panic(fmt.Sprintf("Env variable %s does not exist", s))
		}
	}

	var awsConfig aws.Config
	var err error

	if endpointUrl != "" {
		customResolver := aws.EndpointResolverFunc(func(service, region string) (aws.Endpoint, error) {
			return aws.Endpoint{
				PartitionID:   "S3",
				URL:           endpointUrl,
				SigningRegion: os.Getenv("AWS_REGION"),
			}, nil
		})

		awsConfig, err = config.LoadDefaultConfig(
			context.TODO(),
			config.WithEndpointResolver(customResolver))

	} else {
		awsConfig, err = config.LoadDefaultConfig(context.TODO())
	}
	if err != nil {
		log.Fatal(err)
	}
	// Create an Amazon S3 service client
	client := s3.NewFromConfig(awsConfig)
	return &S3Storage{
		bucket: os.Getenv("S3_BUCKET"),
		s3:     client,
		BaseStorage: gostorages.NewBaseStorage(
			os.Getenv("PICFIT_LOCATION"), os.Getenv("PICFIT_BASE_URL")),
	}
}

// Save saves a file at the given path in the bucket
func (s *S3Storage) Save(path string, file gostorages.File) error {
	var input *s3.PutObjectInput
	b, err := file.ReadAll()
	if err != nil {
		return err
	}
	input = &s3.PutObjectInput{
		Bucket:        aws.String(s.bucket),
		Key:           aws.String(path),
		Body:          bytes.NewBuffer(b),
		ContentLength: int64(len(b)),
	}
	_, err = s.s3.PutObject(context.TODO(), input,
		s3.WithAPIOptions(
			v4.SwapComputePayloadSHA256ForUnsignedPayloadMiddleware,
		))
	return err
}

// Exists checks if the given file is in the bucket
func (s *S3Storage) Exists(filepath string) bool {
	_, err := s.stat(filepath)
	if err != nil {
		return false
	}
	return true
}

// Stat returns path metadata.
func (s *S3Storage) stat(path string) (*stat, error) {
	input := &s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(path),
	}
	out, err := s.s3.HeadObject(context.TODO(), input)
	if err != nil {
		return nil, err
	}

	return &stat{
		ModifiedTime: *out.LastModified,
		Size:         out.ContentLength,
	}, nil
}

// Open returns the file content in a dedicated bucket
func (s *S3Storage) Open(filepath string) (gostorages.File, error) {
	input := &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(filepath),
	}
	out, err := s.s3.GetObject(context.TODO(), input)
	if err != nil {
		return nil, err
	}
	return &gostorages.S3StorageFile{
		ReadCloser: out.Body,
		Key: &ss3.Key{
			Key:          filepath,
			LastModified: out.LastModified.Format(time.RFC3339),
			Size:         out.ContentLength,
			ETag:         *out.ETag,
		},
		Storage: s,
	}, nil
}

// Delete deletes path.
func (s *S3Storage) Delete(path string) error {
	input := &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(path),
	}
	_, err := s.s3.DeleteObject(context.TODO(), input)
	return err
}

func (s *S3Storage) ModifiedTime(path string) (time.Time, error) {
	st, err := s.stat(path)
	if err != nil {
		return time.Time{}, err
	}
	return st.ModifiedTime, nil
}

// Size returns the size of the given file
func (s *S3Storage) Size(path string) int64 {
	st, err := s.stat(path)
	if err != nil {
		return 0
	}
	return st.Size
}

// IsNotExist returns a boolean indicating whether the error is known
// to report that a file or directory does not exist.
func (s *S3Storage) IsNotExist(err error) bool {
	var re *http.ResponseError
	if errors.As(err, &re) {
		var gae *smithy.GenericAPIError
		if errors.As(re.Err, &gae) && gae.Code == "NotFound" {
			return true
		}
	}
	return false
}
