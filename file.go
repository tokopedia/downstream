package downstream

import (
	"context"
	"errors"
	"log"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
)

//FileDownstream struct
type FileDownstream struct {
	URI string // this is the local path where images are saved
	Web string // the webserver hostname via which those files will be accessed. used in GetPublicURL
}

//NewFileDownstream constructor
func NewFileDownstream(path string, web string) *FileDownstream {
	log.Println("Intialising file downstream with path ", path)

	// check to see if downstreamURI is valid
	_, err := url.Parse(web)
	if err != nil {
		log.Fatalln("invalid url ", web)
	}

	// the path must be a directory, but if it has a trailing /, remove it
	return &FileDownstream{
		URI: path,
		Web: strings.TrimRight(web, "/"),
	}
}

func (d *FileDownstream) String() string {
	return "using filesystem to cache " + d.URI + " serving from: " + d.Web
}

//Info file info
func (d *FileDownstream) Info(path string) (string, error) {
	_, err := os.Stat(d.URI + path)
	return "", err
}

//Put upload file
func (d *FileDownstream) Put(data *DSData) (string, error) {
	cachePath := filepath.Join(d.URI, data.Path)

	log.Println("using ", cachePath)

	// existence check
	_, err := os.Stat(cachePath)
	if err == nil {
		log.Println("file already exists, skipping ", cachePath)
		return data.Path, nil
	}

	err = os.MkdirAll(path.Dir(cachePath), os.ModeDir|0777)
	if err == nil {
		out, err := os.Create(cachePath)
		if err == nil {
			out.Write(data.Data)
			out.Close()
			log.Println("cached into " + cachePath)
		}
	}
	return cachePath, err
}

//Get Not implemented yet
func (d *FileDownstream) Get(string, string) (string, error) {
	return "", errors.New("Not implemented yet")
}

//PutWithContext Put With Context
func (d *FileDownstream) PutWithContext(ctx context.Context, data *DSData) (string, error) {
	select {
	case <-ctx.Done():
		log.Println(ctx.Err())
	}

	cachePath := filepath.Join(d.URI, data.Path)

	log.Println("using ", cachePath)

	// existence check
	_, err := os.Stat(cachePath)
	if err == nil {
		log.Println("file already exists, skipping ", cachePath)
		return data.Path, nil
	}

	err = os.MkdirAll(path.Dir(cachePath), os.ModeDir|0777)
	if err == nil {
		out, err := os.Create(cachePath)
		if err == nil {
			out.Write(data.Data)
			out.Close()
			log.Println("cached into " + cachePath)
		}
	}
	return cachePath, err
}

// GetPublicURL get file path
func (d *FileDownstream) GetPublicURL(path string) string {
	return d.Web + "/" + path
}

// Move move file from source to dest
func (d *FileDownstream) Move(src string, dest string) (string, error) {
	log.Println("moving ", src, dest)
	return "", nil
}

// Delete file from source
func (d *FileDownstream) Delete(src string) (string, error) {
	log.Println("delete ", src)
	return "", nil
}
