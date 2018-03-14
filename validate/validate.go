package validate

import (
	"fmt"
	"regexp"

	logger "github.com/ipfs/go-log"
)

var (
	alphaNumericRegex = regexp.MustCompile(`^[a-zA-Z]\w{0,143}$`)
	log               = logger.Logger("validate")
)

// ValidName checks for a valid variable name
// names must:
// * start with a letter
// * consist of only alpha-numeric characters and/or underscores
// * have a total length of no more than 144 characters
func ValidName(name string) error {
	if name == "" {
		err := fmt.Errorf("error: name cannot be empty")
		log.Debug(err.Error())
		return err
	}
	if alphaNumericRegex.FindString(name) == "" {
		err := fmt.Errorf("error: illegal name '%s', names must start with a letter and consist of only a-z,0-9, and _. max length 144 characters", name)
		log.Debug(err.Error())
		return err
	}
	return nil
}
