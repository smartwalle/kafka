package kafka

import (
	"crypto/x509"
	"errors"
	"os"

	"github.com/xdg-go/scram"
)

type SCRAMClient struct {
	*scram.Client
	*scram.ClientConversation
	hash scram.HashGeneratorFcn
}

func NewSCRAMClient(hash scram.HashGeneratorFcn) *SCRAMClient {
	return &SCRAMClient{hash: hash}
}

func (c *SCRAMClient) Begin(userName, password, authzID string) error {
	client, err := c.hash.NewClient(userName, password, authzID)
	if err != nil {
		return err
	}
	c.Client = client
	c.ClientConversation = client.NewConversation()
	return nil
}

func (c *SCRAMClient) Step(challenge string) (string, error) {
	return c.ClientConversation.Step(challenge)
}

func (c *SCRAMClient) Done() bool {
	return c.ClientConversation.Done()
}

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
