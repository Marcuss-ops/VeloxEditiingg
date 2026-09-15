package telemetry

import (
	"reflect"
	"strings"
	"testing"

	pb "velox-shared/controltransport/pb"
)

// TestRawExecutionMetricsProtoRoundTrip keeps the typed transport boundary
// structural. Every TaskExecutionMetrics proto field must have a matching
// RawExecutionMetrics JSON field, and every such field must survive the
// worker serializer and the master-compatible deserializer. Local-only
// collector fields are intentionally excluded because they are not part of
// the wire contract.
func TestRawExecutionMetricsProtoRoundTrip(t *testing.T) {
	rawType := reflect.TypeOf(RawExecutionMetrics{})
	rawFields := make(map[string]int, rawType.NumField())
	for i := 0; i < rawType.NumField(); i++ {
		name := strings.Split(rawType.Field(i).Tag.Get("json"), ",")[0]
		if name != "" && name != "-" {
			rawFields[name] = i
		}
	}

	descriptor := (&pb.TaskExecutionMetrics{}).ProtoReflect().Descriptor()
	values := reflect.New(rawType).Elem()
	matchedFields := make(map[string]int, descriptor.Fields().Len())
	matched := 0
	for i := 0; i < descriptor.Fields().Len(); i++ {
		field := descriptor.Fields().Get(i)
		index, ok := rawFields[string(field.Name())]
		if !ok {
			for candidate, candidateIndex := range rawFields {
				if candidate == field.JSONName() {
					index, ok = candidateIndex, true
					break
				}
			}
		}
		if !ok {
			t.Fatalf("proto field %q has no RawExecutionMetrics field", field.Name())
		}

		value := values.Field(index)
		setContractSentinel(t, value, string(field.Name()))
		matchedFields[string(field.Name())] = index
		matched++
	}
	if matched == 0 {
		t.Fatal("TaskExecutionMetrics descriptor has no fields")
	}

	input := values.Interface().(RawExecutionMetrics)
	output := FromProto(input.ToProto())
	for name, index := range matchedFields {
		if got, want := reflect.ValueOf(output).Field(index).Interface(), values.Field(index).Interface(); !reflect.DeepEqual(got, want) {
			t.Errorf("field %q lost across ToProto/FromProto: got=%v want=%v", name, got, want)
		}
	}
}

func setContractSentinel(t *testing.T, value reflect.Value, name string) {
	t.Helper()
	switch value.Kind() {
	case reflect.Bool:
		value.SetBool(true)
	case reflect.Int32:
		value.SetInt(37)
	case reflect.Int64:
		value.SetInt(3701)
	case reflect.Float64:
		value.SetFloat(37.01)
	case reflect.String:
		value.SetString("contract-" + string(name))
	default:
		t.Fatalf("proto-backed RawExecutionMetrics field %q has unsupported kind %s", name, value.Kind())
	}
}
