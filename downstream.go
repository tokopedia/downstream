package downstream

type DSData struct {
	Data     []byte
	Path     string
	MimeType string
	Meta     string
}

type Downstream interface {
	String() string
	Info(string) (string, error) // return meta associated with filepath, stat equivalent
	Put(*DSData) (string, error)
	Move(string, string) (string, error)
	GetPublicURL(string) string
}
