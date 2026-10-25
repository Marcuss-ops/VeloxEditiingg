package enqueue

import "encoding/json"

func cloneRichPayload(value map[string]interface{}) map[string]interface{} {
	encoded, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	var clone map[string]interface{}
	if err := json.Unmarshal(encoded, &clone); err != nil {
		panic(err)
	}
	return clone
}
