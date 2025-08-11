package core

import (
	"context"
	"io"
	"net/url"
	"os"
	"strings"
	"testing"

	"techiecaro/remblob/storage"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/modules/minio"
)

// TestEditor modifies file content for testing
type TestEditor struct {
	newContent string
}

func (e *TestEditor) Edit(filename string) error {
	return os.WriteFile(filename, []byte(e.newContent), 0644)
}

// MinIOTestSetup contains the MinIO test environment
type MinIOTestSetup struct {
	Container  *minio.MinioContainer
	S3Client   *s3.Client
	BucketName string
	Context    context.Context
}

// setupMinIOTestEnvironment creates and configures a MinIO test environment
func setupMinIOTestEnvironment(t *testing.T) *MinIOTestSetup {
	ctx := context.Background()

	// Start MinIO testcontainer
	minioContainer, err := minio.Run(ctx, "minio/minio:RELEASE.2024-01-16T16-07-38Z")
	require.NoError(t, err)

	// Get connection details
	endpoint, err := minioContainer.ConnectionString(ctx)
	require.NoError(t, err)

	// Setup environment variables for remblob
	setupMinIOEnvironment(t, endpoint)

	// Create S3 client for test operations
	s3Client := createTestS3Client(t, ctx, endpoint)

	// Create test bucket
	bucketName := "test-metadata-bucket"
	_, err = s3Client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	require.NoError(t, err)

	return &MinIOTestSetup{
		Container:  minioContainer,
		S3Client:   s3Client,
		BucketName: bucketName,
		Context:    ctx,
	}
}

// setupMinIOEnvironment configures environment variables for remblob
func setupMinIOEnvironment(t *testing.T, endpoint string) {
	// Save original environment variables
	originalVars := map[string]string{
		"AWS_ENDPOINT_URL":         os.Getenv("AWS_ENDPOINT_URL"),
		"AWS_ACCESS_KEY_ID":        os.Getenv("AWS_ACCESS_KEY_ID"),
		"AWS_SECRET_ACCESS_KEY":    os.Getenv("AWS_SECRET_ACCESS_KEY"),
		"AWS_REGION":               os.Getenv("AWS_REGION"),
		"AWS_S3_FORCE_PATH_STYLE": os.Getenv("AWS_S3_FORCE_PATH_STYLE"),
	}

	// Restore environment variables on test completion
	t.Cleanup(func() {
		for key, value := range originalVars {
			if value == "" {
				os.Unsetenv(key)
			} else {
				os.Setenv(key, value)
			}
		}
	})

	// Set MinIO environment variables
	endpointURL := "http://" + endpoint
	os.Setenv("AWS_ENDPOINT_URL", endpointURL)
	os.Setenv("AWS_ACCESS_KEY_ID", "minioadmin")
	os.Setenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
	os.Setenv("AWS_REGION", "us-east-1")
	os.Setenv("AWS_S3_FORCE_PATH_STYLE", "true")

	// Reinitialize remblob's S3 storage with new environment variables
	err := storage.ReinitializeS3StorageForTesting()
	require.NoError(t, err, "Failed to reinitialize S3 storage for testing")
}

// createTestS3Client creates an S3 client for test operations
func createTestS3Client(t *testing.T, ctx context.Context, endpoint string) *s3.Client {
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("minioadmin", "minioadmin", "")),
		config.WithRegion("us-east-1"),
	)
	require.NoError(t, err)

	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
		o.BaseEndpoint = aws.String("http://" + endpoint)
	})
}

// Cleanup terminates the MinIO container
func (m *MinIOTestSetup) Cleanup(t *testing.T) {
	if err := m.Container.Terminate(m.Context); err != nil {
		t.Logf("failed to terminate container: %s", err)
	}
}

// TestS3MetadataPreservation tests metadata preservation using MinIO testcontainer
func TestS3MetadataPreservation(t *testing.T) {
	minioSetup := setupMinIOTestEnvironment(t)
	defer minioSetup.Cleanup(t)

	// Test data
	testContent := `{"message": "original content"}`
	objectKey := "test-file.json"

	// Upload test file with metadata
	_, err := minioSetup.S3Client.PutObject(minioSetup.Context, &s3.PutObjectInput{
		Bucket:       aws.String(minioSetup.BucketName),
		Key:          aws.String(objectKey),
		Body:         strings.NewReader(testContent),
		ContentType:  aws.String("application/json"),
		CacheControl: aws.String("max-age=3600"),
		Metadata: map[string]string{
			"environment": "test",
			"team":        "integration",
		},
	})
	require.NoError(t, err)

	// Verify initial metadata
	headResp, err := minioSetup.S3Client.HeadObject(minioSetup.Context, &s3.HeadObjectInput{
		Bucket: aws.String(minioSetup.BucketName),
		Key:    aws.String(objectKey),
	})
	require.NoError(t, err)
	assert.Equal(t, "application/json", aws.ToString(headResp.ContentType))
	assert.Equal(t, "max-age=3600", aws.ToString(headResp.CacheControl))
	assert.Equal(t, "test", headResp.Metadata["environment"])
	assert.Equal(t, "integration", headResp.Metadata["team"])

	t.Run("SameFileEditing", func(t *testing.T) {
		// Create test editor that modifies content
		testEditor := &TestEditor{
			newContent: `{"message": "modified by same-file edit"}`,
		}

		// Parse URL for remblob
		sourceURL, err := url.Parse("s3://" + minioSetup.BucketName + "/" + objectKey)
		require.NoError(t, err)

		// Edit the same file (source = destination)
		err = Edit(*sourceURL, *sourceURL, testEditor)
		require.NoError(t, err)

		// Verify metadata is preserved after same-file edit
		headRespAfter, err := minioSetup.S3Client.HeadObject(minioSetup.Context, &s3.HeadObjectInput{
			Bucket: aws.String(minioSetup.BucketName),
			Key:    aws.String(objectKey),
		})
		require.NoError(t, err)

		assert.Equal(t, "application/json", aws.ToString(headRespAfter.ContentType))
		assert.Equal(t, "max-age=3600", aws.ToString(headRespAfter.CacheControl))
		assert.Equal(t, "test", headRespAfter.Metadata["environment"])
		assert.Equal(t, "integration", headRespAfter.Metadata["team"])

		// Verify content was actually changed
		getResp, err := minioSetup.S3Client.GetObject(minioSetup.Context, &s3.GetObjectInput{
			Bucket: aws.String(minioSetup.BucketName),
			Key:    aws.String(objectKey),
		})
		require.NoError(t, err)
		defer getResp.Body.Close()

		content, err := io.ReadAll(getResp.Body)
		require.NoError(t, err)
		assert.Contains(t, string(content), "modified by same-file edit")
	})

	t.Run("CopyToDifferentFile", func(t *testing.T) {
		// Reset the original file content for this test
		_, err := minioSetup.S3Client.PutObject(minioSetup.Context, &s3.PutObjectInput{
			Bucket:       aws.String(minioSetup.BucketName),
			Key:          aws.String(objectKey),
			Body:         strings.NewReader(testContent),
			ContentType:  aws.String("application/json"),
			CacheControl: aws.String("max-age=3600"),
			Metadata: map[string]string{
				"environment": "test",
				"team":        "integration",
			},
		})
		require.NoError(t, err)

		// Create test editor that modifies content
		testEditor := &TestEditor{
			newContent: `{"message": "modified by copy edit"}`,
		}

		// Parse URLs for remblob
		sourceURL, err := url.Parse("s3://" + minioSetup.BucketName + "/" + objectKey)
		require.NoError(t, err)

		copyKey := "test-file-copy.json"
		destURL, err := url.Parse("s3://" + minioSetup.BucketName + "/" + copyKey)
		require.NoError(t, err)

		// Edit to different file (copy)
		err = Edit(*sourceURL, *destURL, testEditor)
		require.NoError(t, err)

		// Verify metadata is transferred to the copy
		headRespCopy, err := minioSetup.S3Client.HeadObject(minioSetup.Context, &s3.HeadObjectInput{
			Bucket: aws.String(minioSetup.BucketName),
			Key:    aws.String(copyKey),
		})
		require.NoError(t, err)

		assert.Equal(t, "application/json", aws.ToString(headRespCopy.ContentType))
		assert.Equal(t, "max-age=3600", aws.ToString(headRespCopy.CacheControl))
		assert.Equal(t, "test", headRespCopy.Metadata["environment"])
		assert.Equal(t, "integration", headRespCopy.Metadata["team"])

		// Verify content was actually changed in the copy
		getResp, err := minioSetup.S3Client.GetObject(minioSetup.Context, &s3.GetObjectInput{
			Bucket: aws.String(minioSetup.BucketName),
			Key:    aws.String(copyKey),
		})
		require.NoError(t, err)
		defer getResp.Body.Close()

		content, err := io.ReadAll(getResp.Body)
		require.NoError(t, err)
		assert.Contains(t, string(content), "modified by copy edit")

		// Verify original file is unchanged
		getOrigResp, err := minioSetup.S3Client.GetObject(minioSetup.Context, &s3.GetObjectInput{
			Bucket: aws.String(minioSetup.BucketName),
			Key:    aws.String(objectKey),
		})
		require.NoError(t, err)
		defer getOrigResp.Body.Close()

		origContent, err := io.ReadAll(getOrigResp.Body)
		require.NoError(t, err)
		assert.Contains(t, string(origContent), "original content")
	})
}
