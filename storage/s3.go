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

type s3Lister interface {
	ListBuckets(context.Context, *s3.ListBucketsInput, ...func(*s3.Options)) (*s3.ListBucketsOutput, error)
	ListObjectsV2(context.Context, *s3.ListObjectsV2Input, ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
}

func getS3FileStorage(uri url.URL, client *s3.Client) *s3FileStorage {
	fs := new(s3FileStorage)
	fs.client = client
	fs.bucket = uri.Host
	fs.key = strings.TrimLeft(uri.Path, "/")
	fs.readBlob = nil
	return fs
}

func buildS3Client() (*s3.Client, error) {
	cfg, err := buildS3Config()
	if err != nil {
		return nil, err
	}

	client := s3.NewFromConfig(cfg)
	return client, nil
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

func s3FileStorageLister(prefix url.URL, client s3Lister) []url.URL {
	suggestions := []url.URL{}

	delimiter := "/"

	// Suggesting buckets
	if prefix.Path == "" {
		buckets, err := client.ListBuckets(context.TODO(), nil)
		if err != nil {
			return suggestions
		}
		for _, bucket := range buckets.Buckets {
			bucketURL := url.URL{
				Scheme: prefix.Scheme,
				Host:   *bucket.Name,
				Path:   delimiter,
			}
			suggestions = append(suggestions, bucketURL)
		}
		return suggestions
	}

	// Suggesting keys in a bucket
	s3Prefix := strings.TrimPrefix(prefix.Path, delimiter)
	params := s3.ListObjectsV2Input{
		Bucket:    &prefix.Host,
		Prefix:    &s3Prefix,
		Delimiter: &delimiter,
	}
	objects, err := client.ListObjectsV2(context.TODO(), &params)
	if err != nil {
		return suggestions
	}

	for _, objectPrefix := range objects.CommonPrefixes {
		folderURL := url.URL{
			Scheme: prefix.Scheme,
			Host:   prefix.Host,
			Path:   *objectPrefix.Prefix,
		}
		suggestions = append(suggestions, folderURL)
	}
	for _, object := range objects.Contents {
		objectURL := url.URL{
			Scheme: prefix.Scheme,
			Host:   prefix.Host,
			Path:   *object.Key,
		}
		suggestions = append(suggestions, objectURL)
	}

	return suggestions
}

func init() {
	client, err := buildS3Client()
	if err != nil {
		log.Fatalf("S3 not available. Could not construct client: %v", err)
		return
	}

	registerFileStorage(
		registrationInfo{
			storage:           func(uri url.URL) FileStorage { return getS3FileStorage(uri, client) },
			lister:            func(prefix url.URL) []url.URL { return s3FileStorageLister(prefix, client) },
			prefixes:          []string{"s3://"},
			completionPrompts: []string{},
		},
	)
}
