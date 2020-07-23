package vfsindex

var Opt optionState

type optionState struct {
	column           string
	output           Outputer
	rootDir          string
	idxMergeOnSearch bool
	cntConcurrent    int
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

// MergeOnSearch ... enable to merge index on search
func MergeOnSearch(enable bool) Option {
	return func(opt *optionState) {
		opt.idxMergeOnSearch = enable
	}
}

func RegitConcurrent(n int) Option {
	return func(opt *optionState) {
		opt.cntConcurrent = n
	}
}
