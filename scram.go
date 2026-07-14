package kafka

import (
	"github.com/xdg-go/scram"
)

type SCRAMClient struct {
	client       *scram.Client
	conversation *scram.ClientConversation
	hash         scram.HashGeneratorFcn
}

func NewSCRAMClient(hash scram.HashGeneratorFcn) *SCRAMClient {
	return &SCRAMClient{hash: hash}
}

func (c *SCRAMClient) Begin(username, password, authzId string) error {
	client, err := c.hash.NewClient(username, password, authzId)
	if err != nil {
		return err
	}
	c.client = client
	c.conversation = client.NewConversation()
	return nil
}

func (c *SCRAMClient) Step(challenge string) (string, error) {
	return c.conversation.Step(challenge)
}

func (c *SCRAMClient) Done() bool {
	return c.conversation.Done()
}
