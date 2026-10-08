package app

import (
	"errors"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"unicode"
	"unicode/utf8"
)

const (
	maxUsernameBytes       = 128
	maxPasswordBytes       = 256
	maxCredentialPathBytes = 2048
)

func (l *Local) credentials() *Credentials {
	if l.Backend.MongoDB != nil {
		return &l.Backend.MongoDB.Credentials
	}
	if l.Backend.Search != nil && l.Backend.Search.Connection != nil {
		return &l.Backend.Search.Connection.Credentials
	}
	return nil
}

func (cfg RoutingConfig) validateCredentialSources() error {
	for _, service := range cfg.Stores {
		if m := service.Local.Backend.MongoDB; m != nil {
			parsed, err := url.Parse(m.URI)
			if err != nil || parsed.User != nil {
				return errors.New("MongoDB URI must not contain credentials")
			}
		}
		if credentials := service.Local.credentials(); credentials != nil {
			if err := credentials.validateSources(); err != nil {
				return err
			}
		}
	}
	return nil
}

func (c Credentials) validateSources() error {
	if c.Username != "" && c.UsernameFile != "" || c.Password != "" && c.PasswordFile != "" {
		return errors.New("credential value and file are mutually exclusive")
	}
	usernameSet := c.Username != "" || c.UsernameFile != ""
	passwordSet := c.Password != "" || c.PasswordFile != ""
	if usernameSet != passwordSet {
		return errors.New("credentials require both username and password sources")
	}
	if c.Username != "" && !validCredentialText(c.Username, maxUsernameBytes) ||
		c.Password != "" && !validCredentialText(c.Password, maxPasswordBytes) {
		return errors.New("invalid credential value")
	}
	for _, filename := range []string{c.UsernameFile, c.PasswordFile} {
		if filename != "" && (strings.TrimSpace(filename) == "" || !validCredentialText(filename, maxCredentialPathBytes)) {
			return errors.New("invalid credential file configuration")
		}
	}
	return nil
}

func validCredentialText(value string, maxBytes int) bool {
	if value == "" || len(value) > maxBytes || !utf8.ValidString(value) {
		return false
	}
	for _, char := range value {
		if unicode.IsControl(char) {
			return false
		}
	}
	return true
}

func (cfg RoutingConfig) resolveCredentials(directory string) error {
	if err := cfg.validateStores(); err != nil {
		return err
	}
	// Validate every source first, so a later conflict cannot trigger earlier IO.
	if err := cfg.validateCredentialSources(); err != nil {
		return err
	}
	for _, service := range cfg.Stores {
		if credentials := service.Local.credentials(); credentials != nil {
			if err := credentials.resolve(directory); err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *Credentials) resolve(directory string) error {
	username, err := resolveCredentialValue(c.Username, c.UsernameFile, directory, maxUsernameBytes)
	if err != nil {
		return err
	}
	password, err := resolveCredentialValue(c.Password, c.PasswordFile, directory, maxPasswordBytes)
	if err != nil {
		return err
	}
	c.Username, c.Password = username, password
	c.UsernameFile, c.PasswordFile = "", ""
	return nil
}

func resolveCredentialValue(value, filename, directory string, maxBytes int) (string, error) {
	if filename == "" {
		return value, nil
	}
	if !filepath.IsAbs(filename) {
		filename = filepath.Join(directory, filename)
	}
	info, err := os.Stat(filename)
	if err != nil || !info.Mode().IsRegular() {
		return "", errors.New("credential file unavailable")
	}
	file, err := os.Open(filename)
	if err != nil {
		return "", errors.New("credential file unavailable")
	}
	info, err = file.Stat()
	if err != nil || !info.Mode().IsRegular() {
		_ = file.Close()
		return "", errors.New("credential file unavailable")
	}
	raw, readErr := io.ReadAll(io.LimitReader(file, int64(maxBytes+3)))
	closeErr := file.Close()
	if readErr != nil || closeErr != nil {
		return "", errors.New("credential file unavailable")
	}
	if len(raw) > maxBytes+2 {
		return "", errors.New("invalid credential file content")
	}
	text := strings.TrimSuffix(string(raw), "\n")
	if len(text) != len(raw) {
		text = strings.TrimSuffix(text, "\r")
	}
	if !validCredentialText(text, maxBytes) {
		return "", errors.New("invalid credential file content")
	}
	return text, nil
}
