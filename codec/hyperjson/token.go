package hyperjson

var errToken = Token{Type: TypeError}

type Token struct {
	Type  Type
	Value []byte
}
