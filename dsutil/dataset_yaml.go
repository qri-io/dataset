package dsutil

import (
	"fmt"

	"github.com/qri-io/dataset"
	"gopkg.in/yaml.v2"
)

// UnmarshalYAMLDatasetPod reads yaml bytes into a DatasetPod
func UnmarshalYAMLDatasetPod(data []byte, ds *dataset.DatasetPod) error {
	if err := yaml.Unmarshal(data, ds); err != nil {
		return err
	}
	if ds.Structure != nil && ds.Structure.Schema != nil {
		for key, val := range ds.Structure.Schema {
			ds.Structure.Schema[key] = cleanupMapValue(val)
		}
	}
	return nil
}

// Unmarshal YAML to map[string]interface{} instead of map[interface{}]interface{}.
// func Unmarshal(in []byte, out interface{}) error {
// 	var res interface{}

// 	if err := yaml.Unmarshal(in, &res); err != nil {
// 		return err
// 	}
// 	*out.(*interface{}) = cleanupMapValue(res)

// 	return nil
// }

// Marshal YAML wrapper function.
// func Marshal(in interface{}) ([]byte, error) {
// 	return yaml.Marshal(in)
// }

func cleanupInterfaceArray(in []interface{}) []interface{} {
	res := make([]interface{}, len(in))
	for i, v := range in {
		res[i] = cleanupMapValue(v)
	}
	return res
}

func cleanupInterfaceMap(in map[interface{}]interface{}) map[string]interface{} {
	res := make(map[string]interface{})
	for k, v := range in {
		res[fmt.Sprintf("%v", k)] = cleanupMapValue(v)
	}
	return res
}

func cleanupMapValue(v interface{}) interface{} {
	switch v := v.(type) {
	case []interface{}:
		return cleanupInterfaceArray(v)
	case map[interface{}]interface{}:
		return cleanupInterfaceMap(v)
	case string, bool, int, int16, int32, int64, float32, float64, []byte:
		return v
	default:
		return fmt.Sprintf("%v", v)
	}
}
