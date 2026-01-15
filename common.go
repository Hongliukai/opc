package opc

import (
	"fmt"
	"time"
)

const (
	ReadModeSingle = 0

	ReadModeMultiLowerBound1 = 1

	ReadModeMultiLowerBound0 = 2
)

type Config struct {
	Mode                int
	ReadSource          int32 // OPCDevice or OPCCache
	TagsCache           bool
	TagsCacheSyncPeriod time.Duration
}

var OPCConfig = &Config{
	Mode:                ReadModeSingle,
	ReadSource:          OPCCache,
	TagsCache:           false,
	TagsCacheSyncPeriod: 1 * time.Minute,
}

type ErrorCode struct {
	Code    uint32
	Message string
}

var (
	OPCInvalidHandle = ErrorCode{
		Code:    0xC0040001,
		Message: "The value of the handle is invalid.",
	}
	OPCBadType = ErrorCode{
		Code:    0xC0040004,
		Message: "The server cannot convert the data between the specified format/ requested data type and the canonical data type.",
	}
	OPCPublic = ErrorCode{
		Code:    0xC0040005,
		Message: "The requested operation cannot be done on a public group.",
	}
	OPCBadRights = ErrorCode{
		Code:    0xC0040006,
		Message: "The Items AccessRights do not allow the operation.",
	}
	OPCUnknownItemID = ErrorCode{
		Code:    0xC0040007,
		Message: "The item ID is not defined in the server address space or no longer exists in the server address space.",
	}
	OPCInvalidItemID = ErrorCode{
		Code:    0xC0040008,
		Message: "The item ID doesn't conform to the server's syntax.",
	}
	OPCInvalidFilter = ErrorCode{
		Code:    0xC0040009,
		Message: "The filter string was not valid.",
	}
	OPCUnknownPath = ErrorCode{
		Code:    0xC004000A,
		Message: "The item's access path is not known to the server.",
	}
	OPCRange = ErrorCode{
		Code:    0xC004000B,
		Message: "The value was out of range.",
	}
	OPCDuplicateName = ErrorCode{
		Code:    0xC004000C,
		Message: "Duplicate name not allowed.",
	}
	OPCUnsupportedRate = ErrorCode{
		Code:    0x0004000D,
		Message: "The server does not support the requested data rate but will use the closest available rate.",
	}
	OPCClamp = ErrorCode{
		Code:    0x0004000E,
		Message: "A value passed to WRITE was accepted but the output was clamped.",
	}
	OPCInuse = ErrorCode{
		Code:    0x0004000F,
		Message: "The operation cannot be performed because the object is being referenced.",
	}
	OPCInvalidConfig = ErrorCode{
		Code:    0xC0040010,
		Message: "The server's configuration file is an invalid format.",
	}
	OPCNotFound = ErrorCode{
		Code:    0xC0040011,
		Message: "Requested Object (e.g. a public group) was not found.",
	}
	OPCInvalidPID = ErrorCode{
		Code:    0xC0040203,
		Message: "The passed property ID is not valid for the item.",
	}
)

func GetErrorMessage(code int32) string {
	var errorCodes = []ErrorCode{
		OPCInvalidHandle,
		OPCBadType,
		OPCPublic,
		OPCBadRights,
		OPCUnknownItemID,
		OPCInvalidItemID,
		OPCInvalidFilter,
		OPCUnknownPath,
		OPCRange,
		OPCDuplicateName,
		OPCUnsupportedRate,
		OPCClamp,
		OPCInuse,
		OPCInvalidConfig,
		OPCNotFound,
		OPCInvalidPID,
	}

	for _, err := range errorCodes {
		if err.Code == uint32(code) {
			return err.Message
		}
	}

	return fmt.Sprintf("0x%XL", code)
}

func GetReadSourceMeaning(source int32) string {
	switch source {
	case OPCDevice:
		return "OPC Device"
	case OPCCache:
		return "OPC Cache"
	default:
		return "Unknown"
	}
}
