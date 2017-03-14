package services

type ErrorFilter struct {
	Tolerances int
	Messages   chan string
}

func (e *ErrorFilter) Quit(err error) (n bool) {
	if err == nil {
		return false
	}
	if edm, ok := err.(ErrDatabaseMissing); ok {
		if edm.UnderlyingErr == nil {
			return false
		}
		e.Messages <- edm.Error()
		return e.Tolerances&ConnectionErrors != 0
	}
	if ep, ok := err.(ErrParsing); ok {
		if ep.UnderlyingErr == nil {
			return false
		}
		e.Messages <- ep.Error()
		return e.Tolerances&ParsingErrors != 0
	}
	return e.Tolerances&UnknownErrors != 0
}

func prettyErr(e error) string {
	if e == nil {
		return "no error"
	}
	return e.Error()
}

type ErrDatabaseMissing struct {
	Name          string
	UnderlyingErr error
}

func (e ErrDatabaseMissing) Error() string {
	return "db conn err with " + e.Name + ": " + prettyErr(e.UnderlyingErr)
}

type ErrParsing struct {
	What          string
	UnderlyingErr error
}

func (e ErrParsing) Error() string {
	return "parsing " + e.What + " err: " + prettyErr(e.UnderlyingErr)
}

type ErrLaunching struct {
	UnderlyingErr error
}

func (e ErrLaunching) Error() string {
	return "failed to launch because " + prettyErr(e.UnderlyingErr)
}

const (
	UnknownErrors = 1 << iota
	ConnectionErrors
	ParsingErrors
)
