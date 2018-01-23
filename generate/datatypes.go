package generate

// import (
// 	"math/rand"
// 	"net/url"
// 	"time"

// 	"github.com/qri-io/dataset/vals"
// )

// // RandomValue generates a randomized value for a given datatype
// func RandomValue(t vals.Type) interface{} {
// 	switch t {
// 	// case vals.Unknown:
// 	// return RandomValue(vals.Type((rand.Intn(vals.NumDatatypes) + 1) - 2))
// 	case vals.TypeString:
// 		return randString(rand.Intn(100))
// 	case vals.TypeNumber:
// 		return rand.Float32()
// 	case vals.Integer:
// 		return rand.Int()
// 	case vals.Boolean:
// 		return rand.Intn(10) > 4
// 	case vals.JSON:
// 		if rand.Intn(2) == 1 {
// 			return map[string]interface{}{}
// 		}
// 		return []interface{}{}
// 	case vals.Date:
// 		return time.Now().Add(time.Hour * 24 * time.Duration(rand.Intn(30)+1))
// 	case vals.URL:
// 		return &url.URL{
// 			Scheme: "http",
// 			Host:   "bit.ly",
// 			Path:   randString(6),
// 		}
// 	}

// 	return nil
// }

// // RandomStringValue is RandomValue that always returns a string
// func RandomStringValue(t vals.Type) string {
// 	switch t {
// 	case vals.Unknown:
// 		return ""
// 	case vals.Any:
// 		return RandomStringValue(vals.Type((rand.Intn(vals.NumDatatypes) + 1) - 2))
// 	case vals.TypeString:
// 		return randString(rand.Intn(100))
// 	case vals.TypeNumber:
// 		str, _ := vals.TypeNumber.ValueToString(rand.Float32())
// 		return str
// 	case vals.Integer:
// 		str, _ := vals.Integer.ValueToString(rand.Int())
// 		return str
// 	case vals.Boolean:
// 		if rand.Intn(10) > 4 {
// 			return "true"
// 		}
// 		return "false"
// 	case vals.JSON:
// 		if rand.Intn(2) > 1 {
// 			return "{}"
// 		}
// 		return "[]"
// 	case vals.Date:
// 		return time.Now().Add(time.Hour * 24 * time.Duration(rand.Intn(30)+1)).Format(time.ANSIC)
// 	case vals.URL:
// 		return "http://bit.ly/" + randString(6)
// 	}

// 	return ""
// }
