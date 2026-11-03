package app

import (
	"errors"
	"path/filepath"
	"strings"

	"github.com/batchstream/weir/internal/backend"
)

// resolve replaces credential files with their contents; the backends validate
// the resulting values.
func (c *Credentials) resolve(directory string) error {
	username, err := credentialValue(c.Username, c.UsernameFile, directory)
	if err != nil {
		return err
	}
	password, err := credentialValue(c.Password, c.PasswordFile, directory)
	if err != nil {
		return err
	}
	c.Username, c.Password = username, password
	c.UsernameFile, c.PasswordFile = "", ""
	return nil
}

func credentialValue(value, filename, directory string) (string, error) {
	if filename == "" {
		return value, nil
	}
	if value != "" {
		return "", errors.New("credential value and file are mutually exclusive")
	}
	if !filepath.IsAbs(filename) {
		filename = filepath.Join(directory, filename)
	}
	raw, err := backend.ReadRegularFile(filename)
	if err != nil {
		return "", errors.New("credential file unavailable")
	}
	text := strings.TrimSuffix(string(raw), "\n")
	if len(text) != len(raw) {
		text = strings.TrimSuffix(text, "\r")
	}
	if text == "" {
		return "", errors.New("invalid credential file content")
	}
	return text, nil
}
