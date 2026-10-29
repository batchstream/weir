package app

import (
	"errors"
	"path/filepath"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/batchstream/weir/internal/backend"
)

func (c Credentials) validateSources() error {
	if c.Username != "" && c.UsernameFile != "" || c.Password != "" && c.PasswordFile != "" {
		return errors.New("credential value and file are mutually exclusive")
	}
	usernameSet := c.Username != "" || c.UsernameFile != ""
	passwordSet := c.Password != "" || c.PasswordFile != ""
	if usernameSet != passwordSet {
		return errors.New("credentials require both username and password sources")
	}
	if c.Username != "" && !validCredentialText(c.Username) ||
		c.Password != "" && !validCredentialText(c.Password) {
		return errors.New("invalid credential value")
	}
	for _, filename := range []string{c.UsernameFile, c.PasswordFile} {
		if filename != "" && (strings.TrimSpace(filename) == "" || !validCredentialText(filename)) {
			return errors.New("invalid credential file configuration")
		}
	}
	return nil
}

func validCredentialText(value string) bool {
	if value == "" || !utf8.ValidString(value) {
		return false
	}
	for _, char := range value {
		if unicode.IsControl(char) {
			return false
		}
	}
	return true
}

func (c *Credentials) resolve(directory string) error {
	username, err := resolveCredentialValue(c.Username, c.UsernameFile, directory)
	if err != nil {
		return err
	}
	password, err := resolveCredentialValue(c.Password, c.PasswordFile, directory)
	if err != nil {
		return err
	}
	c.Username, c.Password = username, password
	c.UsernameFile, c.PasswordFile = "", ""
	return nil
}

func resolveCredentialValue(value, filename, directory string) (string, error) {
	if filename == "" {
		return value, nil
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
	if !validCredentialText(text) {
		return "", errors.New("invalid credential file content")
	}
	return text, nil
}
