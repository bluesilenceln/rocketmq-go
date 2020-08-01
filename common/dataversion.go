package common

import "go.uber.org/atomic"

type DataVersion struct {
	timestamp int64
	counter atomic.Int64
}

