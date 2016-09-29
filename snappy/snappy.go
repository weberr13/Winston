package snappy

import (
	SNAP "github.com/golang/snappy"
)

//CompressBytes using snappy compresion
func CompressBytes(src []byte) []byte {
	return SNAP.Encode(nil, src)
}

//DecompressBytes using snappy compression
func DecompressBytes(src []byte) ([]byte, error) {
	return SNAP.Decode(nil, src)
}
