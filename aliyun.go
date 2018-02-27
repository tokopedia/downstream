package downstream

import (
	"bytes"
	"context"
	"errors"
	"log"
	"path/filepath"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
)

type AliyunDownstream struct {
	client *oss.Client
	b      *oss.Bucket
	bucket string
	prefix string
	Web    string
}

func NewAliyunDownstream(bucket string, path string, web string, endpoint string, key string, secret string) *AliyunDownstream {

	client, err := oss.New(endpoint, key, secret)
	if err != nil {
		log.Fatalln(err)
	}

	b, err := client.Bucket(bucket)
	if err != nil {
		log.Fatalln(err)
	}

	d := &AliyunDownstream{
		client: client,
		prefix: path,
		bucket: bucket,
		Web:    web,
		b:      b,
	}
	return d
}

func (d *AliyunDownstream) String() string {
	return "oss://" + d.bucket
}

func (d *AliyunDownstream) Put(data *DSData) (string, error) {
	cachePath := filepath.Join(d.prefix, data.Path)
	err := d.b.PutObject(cachePath, bytes.NewReader(data.Data))
	return data.Path, err
}

func (d *AliyunDownstream) Move(srcfile string, destfile string) (string, error) {
	return "", errors.New("Not implemented yet")
}

func (d *AliyunDownstream) PutWithContext(ctx context.Context, data *DSData) (string, error) {
	return "", errors.New("Aliyun sdk doesent support put with context")
}

func (d *AliyunDownstream) Info(path string) (string, error) {
	cachePath := filepath.Join(d.prefix, path)
	exists, err := d.b.IsObjectExist(cachePath)
	if !exists {
		err = errors.New("File not found")
	}
	return "", err
}

func (d *AliyunDownstream) GetPublicURL(path string) string {
	return d.Web + "/" + path
}
