//go:build !linux && !darwin

package main

import (
	"errors"
	"os"
)

func evidencePipe(_ *os.File) (*os.File, error) {
	return nil, errors.New("resource helper stdio requires Linux or Darwin pipes")
}

func checkObservationInput(_ *os.File) error {
	return errors.New("resource helper control requires Linux or Darwin pipes")
}
