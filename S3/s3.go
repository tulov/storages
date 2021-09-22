package S3

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
	"github.com/tulov/storages"
	"io"
	"log"
	"mime"
	"os"
	"path/filepath"
)

// Storage is a S3 storage.
type Storage struct {
	bucket string
	s3     *s3.Client
}

// Config is the configuration for Storage.
type Config struct {
	Bucket      string
	EndpointUrl string
}

func NewStorage(cfg Config) (*Storage, error) {
	envKeys := []string{
		"AWS_ACCESS_KEY_ID",
		"AWS_SECRET_ACCESS_KEY",
		"AWS_REGION",
	}
	for _, s := range envKeys {
		tmp := os.Getenv(s)
		if tmp == "" {
			return nil, fmt.Errorf("Env variable %s does not exist", s)
		}
	}

	var awsConfig aws.Config
	var err error

	if cfg.EndpointUrl != "" {
		customResolver := aws.EndpointResolverFunc(func(service, region string) (aws.Endpoint, error) {
			return aws.Endpoint{
				PartitionID:   "S3",
				URL:           cfg.EndpointUrl,
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
	return &Storage{
		bucket: cfg.Bucket,
		s3:     client,
	}, nil
}

// Save saves content to path.
func (s *Storage) Save(ctx context.Context, content io.Reader, path string) error {
	var input *s3.PutObjectInput
	if _, ok := content.(io.Seeker); !ok {
		b, err := io.ReadAll(content)
		if err != nil {
			return err
		}
		input = &s3.PutObjectInput{
			Bucket:        aws.String(s.bucket),
			Key:           aws.String(path),
			Body:          bytes.NewBuffer(b),
			ContentLength: int64(len(b)),
		}
		_, err = s.s3.PutObject(ctx, input,
			s3.WithAPIOptions(
				v4.SwapComputePayloadSHA256ForUnsignedPayloadMiddleware,
			))
		return err
	}
	input = &s3.PutObjectInput{
		Bucket:      aws.String(s.bucket),
		Key:         aws.String(path),
		Body:        content,
		ContentType: aws.String(mime.TypeByExtension(filepath.Ext(path))),
	}

	_, err := s.s3.PutObject(ctx, input)
	return err
}

// Stat returns path metadata.
func (s *Storage) Stat(ctx context.Context, path string) (*storages.Stat, error) {
	input := &s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(path),
	}
	out, err := s.s3.HeadObject(ctx, input)
	if err != nil {
		var re *http.ResponseError
		if errors.As(err, &re) {
			var gae *smithy.GenericAPIError
			if errors.As(re.Err, &gae) && gae.Code == "NotFound" {
				return nil, storages.ErrNotExist
			}
		}
	} else if err != nil {
		return nil, err
	}

	return &storages.Stat{
		ModifiedTime: *out.LastModified,
		Size:         out.ContentLength,
	}, nil
}

// Open opens path for reading.
func (s *Storage) Open(ctx context.Context, path string) (io.ReadCloser, error) {
	input := &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(path),
	}
	out, err := s.s3.GetObject(ctx, input)
	if err != nil {
		var re *http.ResponseError
		if errors.As(err, &re) {
			var gae *smithy.GenericAPIError
			if errors.As(re.Err, &gae) && gae.Code == "NoSuchKey" {
				return nil, storages.ErrNotExist
			}
		}
	} else if err != nil {
		return nil, err
	}
	return out.Body, nil
}

// Delete deletes path.
func (s *Storage) Delete(ctx context.Context, path string) error {
	input := &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(path),
	}
	_, err := s.s3.DeleteObject(ctx, input)
	return err
}
