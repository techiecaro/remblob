package storage

import (
	"bytes"
	"context"
	"log"
	"net/url"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type s3FileStorage struct {
	key       string
	bucket    string
	client    *s3.Client
	readBlob  *s3.GetObjectOutput
	writeBuff *bytes.Buffer
}

func getS3FileStorage(uri url.URL) *s3FileStorage {
	cfg, err := buildS3Config()
	if err != nil {
		log.Fatalf("failed to load SDK configuration, %v", err)
	}

	client := s3.NewFromConfig(cfg)

	fs := new(s3FileStorage)
	fs.client = client
	fs.bucket = uri.Host
	fs.key = strings.TrimLeft(uri.Path, "/")
	fs.readBlob = nil
	return fs
}

func buildS3Config() (aws.Config, error) {
	customResolver := aws.EndpointResolverFunc(func(service, region string) (aws.Endpoint, error) {
		if awsEndpoint, ok := os.LookupEnv("AWS_ENDPOINT"); ok {
			return aws.Endpoint{
				PartitionID:       "aws",
				URL:               awsEndpoint,
				SigningRegion:     region,
				HostnameImmutable: true, // Bucket name in path not hostname!
			}, nil
		}

		// fallback to default
		return aws.Endpoint{}, &aws.EndpointNotFoundError{}
	})

	return config.LoadDefaultConfig(context.TODO(),
		config.WithEndpointResolver(customResolver),
	)
}

func (s *s3FileStorage) Read(p []byte) (n int, err error) {
	if s.readBlob == nil {
		readBlob, err := s.client.GetObject(
			context.TODO(),
			&s3.GetObjectInput{Bucket: &s.bucket, Key: &s.key},
		)
		if err != nil {
			return 0, err
		}
		s.readBlob = readBlob
	}

	return s.readBlob.Body.Read(p)
}

func (s *s3FileStorage) Write(p []byte) (n int, err error) {
	if s.writeBuff == nil {
		s.writeBuff = &bytes.Buffer{}
	}
	return s.writeBuff.Write(p)
}

func (s *s3FileStorage) putObject() error {
	reader := bytes.NewReader(s.writeBuff.Bytes()) // Somehow seeker is actually needed
	_, err := s.client.PutObject(
		context.TODO(),
		&s3.PutObjectInput{Bucket: &s.bucket, Key: &s.key, Body: reader},
	)
	return err
}

func (s *s3FileStorage) Close() error {
	if s.readBlob != nil {
		if err := s.readBlob.Body.Close(); err != nil {
			return err
		}
		s.readBlob = nil
	}

	if s.writeBuff != nil {
		if err := s.putObject(); err != nil {
			return err
		}
		s.writeBuff = nil
	}
	return nil
}
