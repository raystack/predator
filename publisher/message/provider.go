package message

import "github.com/odpf/predator/protocol"

type Provider struct {
	KeyBuilder   protocol.ProtoBuilder
	ValueBuilder protocol.ProtoBuilder
}

func (p *Provider) Get() (*protocol.Message, error) {
	key, err := p.KeyBuilder.Build()
	if err != nil {
		return nil, err
	}

	value, err := p.ValueBuilder.Build()
	if err != nil {
		return nil, err
	}

	return &protocol.Message{
		Key:   key,
		Value: value,
	}, nil
}
