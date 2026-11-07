package events

import (
	b64 "encoding/base64"
	"encoding/json"
	"fmt"
	pb "github.com/kubemq-io/protobuf/go"
)

type object struct {
	Id         string            `json:"id"`
	Channel    string            `json:"channel,omitempty"`
	ClientId   string            `json:"client_id,omitempty"`
	Metadata   string            `json:"metadata,omitempty"`
	Tags       map[string]string `json:"tags,omitempty"`
	BodyJson   json.RawMessage   `json:"body_json,omitempty"`
	BodyString string            `json:"body_string,omitempty"`
}

func newObject(id, channel, clientID, metadata string, tags map[string]string, body []byte) *object {
	obj := &object{
		Id:         id,
		Channel:    channel,
		ClientId:   clientID,
		Metadata:   metadata,
		Tags:       tags,
		BodyJson:   json.RawMessage{},
		BodyString: "",
	}
	var js json.RawMessage
	if err := json.Unmarshal(body, &js); err == nil {
		obj.BodyJson = js
	} else {
		sDec, err := b64.StdEncoding.DecodeString(string(body))
		if err != nil {
			obj.BodyString = string(body)
		} else {
			obj.BodyString = string(sDec)
		}
	}
	return obj
}

func (o *object) String() string {
	data, _ := json.MarshalIndent(o, "", "    ")
	return string(data)
}

func printEvent(event *pb.Event) {
	fmt.Println(newObject(event.EventID, event.Channel, event.ClientID, event.Metadata, event.Tags, event.Body))
}

// printEventReceive prints a received event, which doesn't name its sender.
func printEventReceive(event *pb.EventReceive) {
	fmt.Println(newObject(event.EventID, event.Channel, "", event.Metadata, event.Tags, event.Body))
}
