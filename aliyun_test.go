package downstream

import (
	"os"
	"testing"
)

func TestAliyunDownstream(t *testing.T) {

	key := os.Getenv("ALIYUN_ACCESS_KEY_ID")
	secret := os.Getenv("ALIYUN_SECRET_ACCESS_KEY")

	if key == "" || secret == "" {
		t.Error("ALIYUN_ACCESS_KEY_ID or ALIYUN_SECRET_ACCESS_KEY is not set")
	}

	ds := NewAliyunDownstream("tokopedia-upload", "video",
		"https://tokopedia-upload.oss-ap-southeast-1.aliyuncs.com/video",
		"oss-ap-southeast-1.aliyuncs.com",
		key, secret)
	t.Log(ds)
	data := getData()
	path, err := ds.Put(data)
	if err != nil {
		t.Error(err)
	}
	t.Log(ds.GetPublicURL(path))
}
