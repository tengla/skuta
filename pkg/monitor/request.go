package monitor

import (
	"github.com/tengla/skuta/pkg/entities"
)

type MonitorRequest struct {
	Kind        entities.KindType `json:"kind"`
	Channel     string            `json:"channel"`
	MaxBodySize int               `json:"max_body_size"`
}

func (mr *MonitorRequest) validate() error {

	if mr.Channel == "" {
		return entities.ErrInvalidChannel
	}

	if mr.MaxBodySize < 0 {
		return entities.ErrInvalidBodySize
	}

	return nil
}
