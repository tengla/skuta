package broker

import (
	"context"
	"encoding/json"
	"fmt"

	natsd "github.com/nats-io/nats-server/v2/server"
)

// clusterStreamDetails lists the streams and their consumers through the
// JetStream API, which the leaders answer. The server's own view (Jsz) only
// has the streams this node holds, and a follower doesn't know who is waiting
// on a consumer, so a channel could look idle and be deleted.
func (s *Service) clusterStreamDetails(ctx context.Context) ([]natsd.StreamDetail, error) {
	var list []natsd.StreamDetail
	for offset := 0; ; {
		var page natsd.JSApiStreamListResponse
		if err := s.apiRequest(ctx, natsd.JSApiStreamList, natsd.JSApiStreamListRequest{ApiPagedRequest: natsd.ApiPagedRequest{Offset: offset}}, &page); err != nil {
			return nil, err
		}
		if page.Error != nil {
			return nil, fmt.Errorf("list streams: %s", page.Error.Description)
		}
		for _, info := range page.Streams {
			detail := natsd.StreamDetail{
				Name:    info.Config.Name,
				Created: info.Created,
				Cluster: info.Cluster,
				Config:  &info.Config,
				State:   info.State,
			}
			if info.State.Consumers > 0 {
				consumers, err := s.clusterConsumers(ctx, info.Config.Name)
				if err != nil {
					return nil, err
				}
				detail.Consumer = consumers
			}
			list = append(list, detail)
		}
		offset += len(page.Streams)
		if len(page.Streams) == 0 || offset >= page.Total {
			return list, nil
		}
	}
}

func (s *Service) clusterConsumers(ctx context.Context, stream string) ([]*natsd.ConsumerInfo, error) {
	var list []*natsd.ConsumerInfo
	for offset := 0; ; {
		var page natsd.JSApiConsumerListResponse
		if err := s.apiRequest(ctx, fmt.Sprintf(natsd.JSApiConsumerListT, stream), natsd.ApiPagedRequest{Offset: offset}, &page); err != nil {
			return nil, err
		}
		if page.Error != nil {
			if page.Error.ErrCode == uint16(natsd.JSStreamNotFoundErr) {
				// Deleted since it was listed.
				return nil, nil
			}
			return nil, fmt.Errorf("list consumers: %s", page.Error.Description)
		}
		list = append(list, page.Consumers...)
		offset += len(page.Consumers)
		if len(page.Consumers) == 0 || offset >= page.Total {
			return list, nil
		}
	}
}

func (s *Service) apiRequest(ctx context.Context, subject string, req, res interface{}) error {
	data, err := json.Marshal(req)
	if err != nil {
		return err
	}
	msg, err := s.adminConn.RequestWithContext(ctx, subject, data)
	if err != nil {
		return err
	}
	return json.Unmarshal(msg.Data, res)
}

// ClusterNodes returns how many nodes this one is connected to, itself
// included. It is 0 when the broker isn't clustered.
func (s *Service) ClusterNodes() int {
	if s.Nats == nil || !s.clustered() {
		return 0
	}
	routes, err := s.Nats.Routez(nil)
	if err != nil {
		return 1
	}
	// A node holds several connections to each of the others.
	names := make(map[string]struct{})
	for _, route := range routes.Routes {
		names[route.RemoteName] = struct{}{}
	}
	return len(names) + 1
}
