package downstream

import (
	"bytes"
	"errors"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"log"
	"path/filepath"
)

type S3Downstream struct {
	client *s3manager.Uploader
	s3svc  *s3.S3
	bucket string
	prefix string
	Web    string
}

const (
	S3InfoHeader  = "Size"
	S3CacheHeader = "Cache-Control"
)

func NewS3Downstream(bucket string, path string, web string) *S3Downstream {
	sess := session.New(&aws.Config{
		Region: aws.String("ap-southeast-1"),
	})

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
		Bucket:                         aws.String(d.bucket),  
		CopySource:                     aws.String(cachePath),
		Key:                            aws.String(destPath), 
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

func (d *S3Downstream) Delete(srcfile string) (error) {
	cachePath := filepath.Join(d.prefix, srcfile)

	params := &s3.DeleteObjectInput{
		Bucket:       aws.String(d.bucket),
		Key:          aws.String(cachePath),
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
