package storage

import (
	"bytes"
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// s3ClientInterface defines the S3 operations we need
type s3ClientInterface interface {
	GetObject(context.Context, *s3.GetObjectInput, ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	PutObject(context.Context, *s3.PutObjectInput, ...func(*s3.Options)) (*s3.PutObjectOutput, error)
}

type s3FileStorage struct {
	key       string
	bucket    string
	client    s3ClientInterface
	readBlob  *s3.GetObjectOutput
	writeBuff *bytes.Buffer

	// Metadata preservation fields
	metadata        map[string]string
	contentType     *string
	cacheControl    *string
	contentEncoding *string
	contentLanguage *string
	expires         *time.Time
}

type s3Lister interface {
	ListBuckets(context.Context, *s3.ListBucketsInput, ...func(*s3.Options)) (*s3.ListBucketsOutput, error)
	ListObjectsV2(context.Context, *s3.ListObjectsV2Input, ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
}

func getS3FileStorage(uri url.URL, client s3ClientInterface) *s3FileStorage {
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
	if _, anonymous := os.LookupEnv("AWS_NO_SIGN_REQUEST"); anonymous {
		client = s3.NewFromConfig(cfg, func(o *s3.Options) {
			o.Credentials = aws.AnonymousCredentials{}
		})
	}

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

		// Capture metadata for preservation
		s.preserveMetadata(readBlob)
	}

	return s.readBlob.Body.Read(p)
}

// preserveMetadata captures metadata from GetObjectOutput for later use in PutObject
func (s *s3FileStorage) preserveMetadata(output *s3.GetObjectOutput) {
	s.metadata = output.Metadata
	s.contentType = output.ContentType
	s.cacheControl = output.CacheControl
	s.contentEncoding = output.ContentEncoding
	s.contentLanguage = output.ContentLanguage
	s.expires = output.Expires
}

func (s *s3FileStorage) Write(p []byte) (n int, err error) {
	if s.writeBuff == nil {
		s.writeBuff = &bytes.Buffer{}
	}
	return s.writeBuff.Write(p)
}

func (s *s3FileStorage) putObject() error {
	reader := bytes.NewReader(s.writeBuff.Bytes()) // Somehow seeker is actually needed

	// Build PutObjectInput with preserved metadata
	putInput := &s3.PutObjectInput{
		Bucket: &s.bucket,
		Key:    &s.key,
		Body:   reader,
	}

	// Apply preserved metadata
	s.applyPreservedMetadata(putInput)

	_, err := s.client.PutObject(context.TODO(), putInput)
	return err
}

// applyPreservedMetadata applies previously captured metadata to PutObjectInput
func (s *s3FileStorage) applyPreservedMetadata(putInput *s3.PutObjectInput) {
	// Apply custom metadata
	if s.metadata != nil {
		putInput.Metadata = s.metadata
	}

	// Apply standard HTTP headers
	if s.contentType != nil {
		putInput.ContentType = s.contentType
	}
	if s.cacheControl != nil {
		putInput.CacheControl = s.cacheControl
	}
	if s.contentEncoding != nil {
		putInput.ContentEncoding = s.contentEncoding
	}
	if s.contentLanguage != nil {
		putInput.ContentLanguage = s.contentLanguage
	}
	if s.expires != nil {
		putInput.Expires = s.expires
	}
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

// GetMetadata implements MetadataCapable interface
func (s *s3FileStorage) GetMetadata() map[string]string {
	result := make(map[string]string)

	// Add custom metadata
	if s.metadata != nil {
		for k, v := range s.metadata {
			result[k] = v
		}
	}

	// Add S3-specific metadata as special keys
	if s.contentType != nil {
		result["__content-type"] = *s.contentType
	}
	if s.cacheControl != nil {
		result["__cache-control"] = *s.cacheControl
	}
	if s.contentEncoding != nil {
		result["__content-encoding"] = *s.contentEncoding
	}
	if s.contentLanguage != nil {
		result["__content-language"] = *s.contentLanguage
	}
	if s.expires != nil {
		result["__expires"] = s.expires.Format(time.RFC3339)
	}

	return result
}

// SetMetadata implements MetadataCapable interface
func (s *s3FileStorage) SetMetadata(metadata map[string]string) error {
	s.metadata = make(map[string]string)

	for k, v := range metadata {
		// Handle S3-specific metadata
		switch k {
		case "__content-type":
			s.contentType = aws.String(v)
		case "__cache-control":
			s.cacheControl = aws.String(v)
		case "__content-encoding":
			s.contentEncoding = aws.String(v)
		case "__content-language":
			s.contentLanguage = aws.String(v)
		case "__expires":
			if t, err := time.Parse(time.RFC3339, v); err == nil {
				s.expires = &t
			}
		default:
			// Regular custom metadata
			s.metadata[k] = v
		}
	}
	return nil
}

// GetVersion implements VersionCapable interface
func (s *s3FileStorage) GetVersion() string {
	if s.readBlob != nil && s.readBlob.ETag != nil {
		return *s.readBlob.ETag
	}
	return ""
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

	// Suggesting "folders"
	for _, objectPrefix := range objects.CommonPrefixes {
		folderURL := url.URL{
			Scheme: prefix.Scheme,
			Host:   prefix.Host,
			Path:   *objectPrefix.Prefix,
		}
		suggestions = append(suggestions, folderURL)
	}
	// Suggesting "files"
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
		fmt.Printf("S3 not available. Could not construct client: %#v\n", err.Error())
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
