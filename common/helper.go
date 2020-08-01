package common

import (
	"context"
	"github.com/golang/protobuf/proto"
	"google.golang.org/grpc/stats"
	"time"
)

func GetRemoteAddr(ctx context.Context) string {
	info, ok := ctx.Value("conn").(*stats.ConnTagInfo)
	if !ok {
		return ""
	}
	return info.RemoteAddr.String()
}

func Deserializable(bytes []byte, m proto.Message, isCompressed bool) error {
	if isCompressed {
	}
	return proto.Unmarshal(bytes, m)
}

func Serializable(m proto.Message) []byte {
	res, _ := proto.Marshal(m)
	return res
}

func CurrentTimeMills() int64 {
	return time.Now().UnixNano() / 1e6
}
