package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	samplePath = "./samples/"
)

func findActualOutput(t *testing.T, path string) []byte {
	opPath := strings.ReplaceAll(path, ".txt", ".out")

	op, err := os.ReadFile(opPath)
	require.NoError(t, err)

	return op
}

func TestProcessor(t *testing.T) {
	paths, err := filepath.Glob(samplePath + "*.txt")
	if err != nil {
		assert.Fail(t, err.Error())
		return
	}

	for _, path := range paths {
		t.Run(path, func(t *testing.T) {
			file, err := os.Open(path)
			require.NoError(t, err)

			var b bytes.Buffer
			op := findActualOutput(t, path)

			result := processFile(file)
			printResult(&b, result)

			require.NoError(t, err)
			assert.Equal(t, op, b.Bytes())
		})
	}
}
