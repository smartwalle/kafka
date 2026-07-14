package kafka

import (
	"crypto/x509"
	"errors"
	"os"
)

func NewCertPool(file string) (*x509.CertPool, error) {
	content, err := os.ReadFile(file)
	if err != nil {
		return nil, err
	}

	var certPool = x509.NewCertPool()
	if ok := certPool.AppendCertsFromPEM(content); !ok {
		return nil, errors.New("parse CA certificate failed")
	}
	return certPool, nil
}
