package datatype_generate

import (
	"math/rand"
	"net/url"
	"time"

	"github.com/qri-io/dataset/datatypes"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func RandomValue(t datatype.Type) interface{} {
	switch t {
	case datatype.Unknown:
		return nil
	case datatype.Any:
		return RandomValue(datatype.Type((rand.Intn(datatype.NUM_DATA_TYPES) + 1) - 2))
	case datatype.String:
		return randString(rand.Intn(100))
	case datatype.Float:
		return rand.Float32()
	case datatype.Integer:
		return rand.Int()
	case datatype.Boolean:
		return rand.Intn(10) > 4
	case datatype.Object:
		return map[string]interface{}{}
	case datatype.Array:
		return []interface{}{}
	case datatype.Date:
		return time.Now().Add(time.Hour * 24 * time.Duration(rand.Intn(30)+1))
	case datatype.Url:
		return &url.URL{
			Scheme: "http",
			Host:   "bit.ly",
			Path:   randString(6),
		}
	}

	return nil
}

func RandomStringValue(t datatype.Type) string {
	switch t {
	case datatype.Unknown:
		return ""
	case datatype.Any:
		return RandomStringValue(datatype.Type((rand.Intn(datatype.NUM_DATA_TYPES) + 1) - 2))
	case datatype.String:
		return randString(rand.Intn(100))
	case datatype.Float:
		str, _ := datatype.Float.ValueToString(rand.Float32())
		return str
	case datatype.Integer:
		str, _ := datatype.Integer.ValueToString(rand.Int())
		return str
	case datatype.Boolean:
		if rand.Intn(10) > 4 {
			return "true"
		} else {
			return "false"
		}
	case datatype.Object:
		return "{}"
	case datatype.Array:
		return "[]"
	case datatype.Date:
		return time.Now().Add(time.Hour * 24 * time.Duration(rand.Intn(30)+1)).Format(time.ANSIC)
	case datatype.Url:
		return "http://bit.ly/" + randString(6)
	}

	return ""
}

var alphaNumericRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

func randString(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = alphaNumericRunes[rand.Intn(len(alphaNumericRunes))]
	}
	return string(b)
}
