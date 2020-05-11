package generate

import (
	"math"
	"math/rand"
)

// ValueGenerator is a state machine for producing values
type ValueGenerator struct {
	Rand            *rand.Rand // random number generator
	MaxStringLength int
}

// Value creates a random value of a random type
func (g *ValueGenerator) Value() interface{} {
	i := g.Rand.Intn(40)
	if i == 0 {
		return nil
	} else if i > 0 && i < 10 {
		return g.Int()
	} else if i > 10 && i < 20 {
		return g.String()
	} else if i > 20 && i < 30 {
		return g.Float()
	} else if i > 30 && i < 40 {
		return g.Bool()
	}

	return nil
}

// Type creates a value to match a string type. type names match the
// JSON-schema specification
func (g *ValueGenerator) Type(t string) interface{} {
	switch t {
	case "string":
		return g.String()
	case "boolean":
		return g.Bool()
	case "number":
		return g.Float()
	case "integer":
		return g.Int()
	case "object":
		return g.Object()
	case "array":
		return g.Array()
	case "null":
		return nil
	default:
		return g.Value()
	}
}

var alphaNumericRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

// String yields a random string
func (g *ValueGenerator) String() string {
	runes := make([]rune, g.Rand.Intn(g.MaxStringLength))
	for i := range runes {
		runes[i] = alphaNumericRunes[g.Rand.Intn(len(alphaNumericRunes))]
	}
	return string(runes)
}

// Float yields a random floating point number
func (g *ValueGenerator) Float() float64 {
	return g.Rand.NormFloat64()
}

// Int yields a random integer
func (g *ValueGenerator) Int() int {
	return g.Rand.Intn(math.MaxInt64)
}

// Bool yields a random coin flip
func (g *ValueGenerator) Bool() bool {
	return g.Rand.Intn(1)%2 == 0
}

// Object creates an empty object
// TODO (b5) - populate with random values
func (g *ValueGenerator) Object() map[string]interface{} {
	return map[string]interface{}{}
}

// Array creates an empty array
// TODO (b5) - populate with random values
func (g *ValueGenerator) Array() []interface{} {
	return []interface{}{}
}
