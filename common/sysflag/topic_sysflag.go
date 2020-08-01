package sysflag

const (
	FlagUnit    = 0x1 << 0
	FlagUnitSub = 0x1 << 1
)

func HasUnitFlag(sysFlag int) bool {
	return (sysFlag & FlagUnit) == FlagUnit
}

func HasUnitSubFlag(sysFlag int) bool {
	return (sysFlag & FlagUnitSub) == FlagUnitSub
}