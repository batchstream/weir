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

func (cfg RoutingConfig) validateCredentialSources() error {
	for _, service := range cfg.Stores {
		if service.Local == nil {
			continue
		}
		if m := service.Local.MongoDB; m != nil {
			parsed, err := url.Parse(m.URI)
			if err != nil || parsed.User != nil {
				return errors.New("MongoDB URI must not contain credentials")
			}
			err = validateCredentialPair(m.Username, m.Password, m.UsernameFile, m.PasswordFile)
			if err != nil {
				return err
			}
		}
		if s := service.Local.Search; s != nil && s.Connection != nil {
			c := s.Connection
			err := validateCredentialPair(c.Username, c.Password, c.UsernameFile, c.PasswordFile)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func validateCredentialPair(username, password, usernameFile, passwordFile string) error {
	if username != "" && usernameFile != "" || password != "" && passwordFile != "" {
		return errors.New("credential value and file are mutually exclusive")
	}
	usernameSet := username != "" || usernameFile != ""
	passwordSet := password != "" || passwordFile != ""
	if usernameSet != passwordSet {
		return errors.New("credentials require both username and password sources")
	}
	if username != "" && !validCredentialText(username, maxUsernameBytes) ||
		password != "" && !validCredentialText(password, maxPasswordBytes) {
		return errors.New("invalid credential value")
	}
	for _, filename := range []string{usernameFile, passwordFile} {
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
		if service.Local == nil {
			continue
		}
		if m := service.Local.MongoDB; m != nil {
			username, err := resolveCredentialValue(m.Username, m.UsernameFile, directory, maxUsernameBytes)
			if err != nil {
				return err
			}
			password, err := resolveCredentialValue(m.Password, m.PasswordFile, directory, maxPasswordBytes)
			if err != nil {
				return err
			}
			m.Username, m.Password = username, password
			m.UsernameFile, m.PasswordFile = "", ""
		}
		if s := service.Local.Search; s != nil && s.Connection != nil {
			c := s.Connection
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
		}
	}
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
