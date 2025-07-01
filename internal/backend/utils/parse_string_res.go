package utils

type ParseStringRes struct {
	Str  string
	Next int
}

func NewParseStringRes(str string, next int) *ParseStringRes {
	return &ParseStringRes{
		Str:  str,
		Next: next,
	}
}
