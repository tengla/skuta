package queries

import (
	b64 "encoding/base64"
	"encoding/json"
	"fmt"

	pb "github.com/kubemq-io/protobuf/go"
	"strconv"
	"time"
)

type object struct {
	Id         string            `json:"id"`
	Channel    string            `json:"channel,omitempty"`
	ClientId   string            `json:"client_id,omitempty"`
	Metadata   string            `json:"metadata,omitempty"`
	Tags       map[string]string `json:"tags,omitempty"`
	Timeout    string            `json:"timeout,omitempty"`
	BodyJson   json.RawMessage   `json:"body_json,omitempty"`
	BodyString string            `json:"body_string,omitempty"`
	Executed   string            `json:"executed,omitempty"`
	ExecutedAt string            `json:"executed_at,omitempty"`
	Error      string            `json:"error,omitempty"`
	CacheHit   string            `json:"cache_hit,omitempty"`
}

func newObjectWithQueryReceive(query *pb.Request) *object {
	obj := &object{
		Id:         query.RequestID,
		Channel:    query.Channel,
		ClientId:   query.ClientID,
		Metadata:   query.Metadata,
		Tags:       query.Tags,
		BodyJson:   json.RawMessage{},
		BodyString: "",
		Executed:   "",
		ExecutedAt: "",
		Error:      "",
		CacheHit:   "",
		Timeout:    "",
	}
	var js json.RawMessage
	if err := json.Unmarshal(query.Body, &js); err == nil {
		obj.BodyJson = js
	} else {
		sDec, err := b64.StdEncoding.DecodeString(string(query.Body))
		if err != nil {
			obj.BodyString = string(query.Body)
		} else {
			obj.BodyString = string(sDec)
		}
	}

	return obj
}
func newObjectWithQueryResponse(response *pb.Response) *object {
	obj := &object{
		Id:         response.RequestID,
		Channel:    "",
		ClientId:   response.ClientID,
		Metadata:   response.Metadata,
		Tags:       response.Tags,
		BodyJson:   json.RawMessage{},
		BodyString: "",
		Executed:   strconv.FormatBool(response.Executed),
		ExecutedAt: time.Unix(response.Timestamp, 0).Format("2006-01-02 15:04:05.999"),
		Error:      response.Error,
		CacheHit:   strconv.FormatBool(response.CacheHit),
	}

	var js json.RawMessage
	if err := json.Unmarshal(response.Body, &js); err == nil {
		obj.BodyJson = js
	} else {
		sDec, err := b64.StdEncoding.DecodeString(string(response.Body))
		if err != nil {
			obj.BodyString = string(response.Body)
		} else {
			obj.BodyString = string(sDec)
		}
	}
	return obj
}
func newObjectWithQuery(query *pb.Request) *object {
	obj := &object{
		Id:         query.RequestID,
		Channel:    query.Channel,
		ClientId:   query.ClientID,
		Metadata:   query.Metadata,
		Tags:       query.Tags,
		Timeout:    (time.Duration(query.Timeout) * time.Millisecond).String(),
		BodyJson:   json.RawMessage{},
		BodyString: "",
		Executed:   "",
		ExecutedAt: "",
		Error:      "",
		CacheHit:   "",
	}

	var js json.RawMessage
	if err := json.Unmarshal(query.Body, &js); err == nil {
		obj.BodyJson = js
	} else {
		sDec, err := b64.StdEncoding.DecodeString(string(query.Body))
		if err != nil {
			obj.BodyString = string(query.Body)
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

func printQueryReceive(query *pb.Request) {
	fmt.Println(newObjectWithQueryReceive(query))
}

func printQueryResponse(response *pb.Response) {
	fmt.Println(newObjectWithQueryResponse(response))
}
func printQuery(query *pb.Request) {
	fmt.Println(newObjectWithQuery(query))
}
