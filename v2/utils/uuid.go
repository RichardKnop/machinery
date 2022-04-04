package utils

import (
	"github.com/google/uuid"
	"strings"
)

func GetPureUUID() string {
	uid, _ := uuid.NewUUID()
	return strings.Replace(uid.String(), "-", "", -1)
}
