package tq

import (
	"context"
	"strings"
)

type serveMux struct {
	m map[string]handler
}

func NewServeMux() *serveMux {
	return &serveMux{
		m: make(map[string]handler),
	}
}

// Dispatches the task to the handler whose pattern matches the task type.
func (s *serveMux) ProcessTask(ctx context.Context, task *Task) *Result {
	handler, ok := s.m[task.Key]
	if !ok {
		return &Result{
			Error: "no handler registered",
		}
	}

	return handler.ProcessTask(ctx, task)
}

// Registers the handler function for the given pattern.
func (s *serveMux) Register(pattern string, h func(context.Context, *Task) *Result) {

	if h == nil {
		panic("taskq: nil handler")
	}
	if strings.TrimSpace(pattern) == "" {
		panic("taskq: invalid pattern")
	}
	if _, exist := s.m[pattern]; exist {
		panic("taskq: multiple registrations for " + pattern)
	}

	if s.m == nil {
		s.m = make(map[string]handler)
	}
	s.m[pattern] = handlerFunc(h)
}
