package vfsindex

var Opt optionState

type optionState struct {
	column  string
	output  Outputer
	rootDir string
}

//type ReaderOpt map[string]string
type Option func(*optionState)

// ReaderColumn ... config for columname for search/read
func ReaderColumn(s string) Option {

	return func(opt *optionState) {
		opt.column = s
	}
}

func mergeOpt(s *optionState, opts ...Option) {

	for _, opt := range opts {
		opt(s)
	}
}

// RootDir ... set index top directory
func RootDir(s string) Option {
	return func(opt *optionState) {
		opt.rootDir = s
	}
}

type Outputer byte

const (
	JsonOutput = iota
	MapInfOutput
)

func Output(t Outputer) Option {
	return func(opt *optionState) {
		opt.output = t
	}
}
