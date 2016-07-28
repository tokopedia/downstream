package downstream

import (
	"net/url"
	"testing"
)

func getData() *DSData {
	data := &DSData{
		Path:     "img/test.txt",
		Data:     []byte("hello world"),
		MimeType: "text/plain",
	}
	return data
}

func TestS3Downstream(t *testing.T) {
	u, err := url.Parse("s3://tokopedia-upload/built")
	t.Log("using prefix", u.Path)

	ds := NewS3Downstream(u.Host, u.Path, "https://ecs7.tokopedia.net/built")
	data := getData()
	path, err := ds.Put(data)
	if err != nil {
		t.Error(err)
	}
	t.Log(ds.GetPublicURL(path))
	_, err = ds.Info(path)
	if err != nil {
		t.Error(err)
	}
}

func TestFileDownstream(t *testing.T) {
	var ds Downstream
	ds = NewFileDownstream("/tmp/test", "https://ecs1.tokopedia.net")
	data := getData()
	path, err := ds.Put(data)
	if err != nil {
		t.Error(err)
	}
	t.Log(path)
}

func TestMoveFileS3Downstream(t *testing.T) {
	u, err := url.Parse("s3://tokopedia-upload")

	ds := NewS3Downstream(u.Host, u.Path, "https://ecs7.tokopedia.net")
	srcfile := "download2.jpg"
	destfile := "download.jpg"

	_, err = ds.Move(srcfile, destfile)
	if err != nil {
		t.Error(err)
	}
}
