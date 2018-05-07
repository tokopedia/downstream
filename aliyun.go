package downstream

import (

	//      "bytes"
	"context"
	"errors"
	"log"
	"path/filepath"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	"github.com/tokopedia/image-quality/src/constants"
)

// AliyunDownstream struct
type AliyunDownstream struct {
	client *oss.Client
	b      *oss.Bucket
	bucket string
	prefix string
	Web    string
}

// NewAliyunDownstream Downstream constructor
func NewAliyunDownstream(bucket string, prefix string, web string, endpoint string, key string, secret string) *AliyunDownstream {

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
		prefix: prefix,
		bucket: bucket,
		Web:    web,
		b:      b,
	}
	return d
}

func (d *AliyunDownstream) String() string {
	return "oss://" + d.bucket
}

// Put upload file to oss
func (d *AliyunDownstream) Put() error {
	err := d.b.UploadFile(constants.OssCachePath, constants.UploadModelFilePath, 100*1024)
	return err
}

// Get Download file to oss
func (d *AliyunDownstream) Get() error {
	err := d.b.GetObjectToFile(constants.OssCachePath, constants.DownloadModelFilePath)
	return err
}

// Move not implemented
func (d *AliyunDownstream) Move(srcfile string, destfile string) (string, error) {
	return "", errors.New("Not implemented yet")
}

// PutWithContext not implemented
func (d *AliyunDownstream) PutWithContext(ctx context.Context, data *DSData) (string, error) {
	return "", errors.New("Aliyun sdk doesent support put with context")
}

// Info not get file info
func (d *AliyunDownstream) Info(path string) error {
	cachePath := filepath.Join(d.prefix, path)
	exists, err := d.b.IsObjectExist(cachePath)
	if !exists {
		err = errors.New("File not found")
	}
	return err
}

// GetPublicURL get oss file url
func (d *AliyunDownstream) GetPublicURL(path string) string {
	return d.Web + "/" + path
}
