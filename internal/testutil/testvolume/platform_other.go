//go:build !linux

package main

import (
	"errors"
	"os"
)

func supported() error                    { return errors.New("native Linux required; unsupported execution refused") }
func openPublic(string) (*os.File, error) { return nil, supported() }
func filesystem(string, bool) (filesystemProof, error) {
	result := filesystemProof{}
	return result, supported()
}
func observeProcess() (processProof, error) {
	result := processProof{}
	return result, supported()
}
