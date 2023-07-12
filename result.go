package tq

import (
	"encoding/json"
	"log"
)

type Result struct {
	Error string  `json:"error"`
	Value string `json:"value"`
}

func (s Result) Encode() map[string]interface{} {

	var values map[string]interface{}

	data, err := json.Marshal(s)
	if err != nil {
		log.Fatalf("Unable to marshall task: %s", err)
	}

	err = json.Unmarshal(data, &values)
	if err != nil {
		panic(err)
	}

	return values
}

func DecodeResult(val map[string]interface{}) *Result {

	jsonbody, err := json.Marshal(val)
	if err != nil {
		panic(err)
	}

	res := Result{}
	if err := json.Unmarshal(jsonbody, &res); err != nil {
		panic(err)
	}

	return &res
}
