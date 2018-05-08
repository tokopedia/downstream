package downstream

import (
	"bytes"
	"context"
	"errors"
	"log"
	"path/filepath"

	"github.com/aws/aws-sdk-go/aws"
	awscreds "github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

type S3Downstream struct {
	client *s3manager.Uploader
	s3svc  *s3.S3
	bucket string
	prefix string
	Web    string
}

type S3DownstreamConfig struct {
	URI          string
	Web          string
	Type         string
	AccessKey    string
	SecretKey    string
	Token        string
	ProviderName string
	Profile      string
	Path         string
}

type CredsProvider awscreds.Provider

const (
	S3InfoHeader     = "Size"
	S3CacheHeader    = "Cache-Control"
	DefaultCredsPath = "/root/.aws/credentials"
)

func GenerateProvider(config *S3DownstreamConfig) CredsProvider {
	var provider CredsProvider

	switch config.Type {
	case "static":
		provider = &awscreds.StaticProvider{
			Value: awscreds.Value{
				AccessKeyID:     config.AccessKey,
				SecretAccessKey: config.SecretKey,
				SessionToken:    config.Token,
				ProviderName:    config.ProviderName,
			},
		}
	case "shared":
		if config.Path == "" {
			config.Path = DefaultCredsPath
		}

		provider = &awscreds.SharedCredentialsProvider{
			Filename: config.Path,
			Profile:  config.Profile,
		}
	}

	return provider
}

func NewS3Downstream(bucket, path, web string, credsProvider ...CredsProvider) *S3Downstream {
	awsConfig := &aws.Config{
		Region: aws.String("ap-southeast-1"),
	}

	if len(credsProvider) == 1 {
		provider := credsProvider[0]
		awsConfig.WithCredentials(awscreds.NewCredentials(provider))
	}

	sess := session.New(awsConfig)

	svc := s3.New(sess)
	if _, err := svc.HeadBucket(&s3.HeadBucketInput{Bucket: &bucket}); err != nil {
		log.Fatal("Failed - no such bucket exist %s, %s\n", bucket, err)
		return nil
	}

	d := &S3Downstream{
		client: s3manager.NewUploader(sess),
		bucket: bucket,
		prefix: path,
		Web:    web,
		s3svc:  svc,
	}
	return d
}

func (d *S3Downstream) String() string {
	return "s3://tokopedia-upload"
}

func (d *S3Downstream) Put(data *DSData) (string, error) {
	cachePath := filepath.Join(d.prefix, data.Path)
	upInput := &s3manager.UploadInput{
		Bucket:      aws.String(d.bucket),
		Key:         aws.String(cachePath),
		Body:        bytes.NewReader(data.Data),
		ContentType: aws.String(data.MimeType),
	}
	_, err := d.client.Upload(upInput)
	return data.Path, err
}

//Get Not implemented yet
func (d *S3Downstream) Get(string, string) (string, error) {
	return "", errors.New("Not implemented yet")
}

func (d *S3Downstream) PutWithContext(ctx context.Context, data *DSData) (string, error) {
	cachePath := filepath.Join(d.prefix, data.Path)
	upInput := &s3manager.UploadInput{
		Bucket:      aws.String(d.bucket),
		Key:         aws.String(cachePath),
		Body:        bytes.NewReader(data.Data),
		ContentType: aws.String(data.MimeType),
	}
	_, err := d.client.UploadWithContext(ctx, upInput)
	return data.Path, err
}

func (d *S3Downstream) Move(srcfile string, destfile string) (string, error) {

	var err error
	cachePath := filepath.Join(d.prefix, srcfile)
	_, err = d.Info(cachePath)
	if err != nil {
		return "", errors.New("File does not exist")
	}

	cachePath = filepath.Join(d.bucket, cachePath)
	destPath := filepath.Join(d.prefix, destfile)
	params := &s3.CopyObjectInput{
		Bucket:     aws.String(d.bucket),
		CopySource: aws.String(cachePath),
		Key:        aws.String(destPath),
	}
	_, err = d.s3svc.CopyObject(params)
	if err != nil {
		return "", errors.New("Copy file failed")
	}

	err = d.Delete(srcfile)

	if err != nil {
		return "", err
	}

	return "", nil
}

func (d *S3Downstream) Delete(srcfile string) error {
	cachePath := filepath.Join(d.prefix, srcfile)

	params := &s3.DeleteObjectInput{
		Bucket: aws.String(d.bucket),
		Key:    aws.String(cachePath),
	}
	_, err := d.s3svc.DeleteObject(params)

	if err != nil {
		return errors.New("Delete file failed")
	}

	return nil
}

func (d *S3Downstream) Info(path string) (string, error) {
	// we  could just do a head request using svc and check for existence
	cachePath := filepath.Join(d.prefix, path)
	resp, err := d.s3svc.HeadObject(&s3.HeadObjectInput{Bucket: aws.String(d.bucket), Key: aws.String(cachePath)})
	if err == nil && *resp.ContentLength == 0 {
		err = errors.New("Content Length 0 for " + path)
	}
	return "", err
}

func (d *S3Downstream) GetPublicURL(path string) string {
	return d.Web + "/" + path
}
