package oci

import (
	//	"errors"
	//	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsTransientOCIErr(t *testing.T) {
	assert.True(t, isTransientOCIErr(assert.AnError))
	//	assert.False(t, isTransientOCIErr())
}
