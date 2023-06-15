package tq

import (
	"context"
	"fmt"
	"strings"
)

type ServeMux struct {
    m map[string]Handler
}

func NewServeMux() *ServeMux {
    return &ServeMux{
        m: make(map[string]Handler),
    }
}

// Dispatches the task to the handler whose pattern matches the task type.
func (s *ServeMux) ProcessTask(ctx context.Context, task *Task) Result {
	handler, ok := s.m[task.Key]
	if !ok {
        return Result{
            Error: fmt.Errorf("no handler registered"),
        }
	}

    return handler.ProcessTask(ctx, task)
}

// Registers the handler function for the given pattern.
func (s *ServeMux) Register(pattern string, handler func(context.Context, *Task) Result) {

	if handler == nil {
		panic("taskq: nil handler")
	}
	if strings.TrimSpace(pattern) == "" {
		panic("taskq: invalid pattern")
	}
	if _, exist := s.m[pattern]; exist {
		panic("taskq: multiple registrations for " + pattern)
	}

	if s.m == nil {
		s.m = make(map[string]Handler)
	}
	s.m[pattern] = HandlerFunc(handler)
}
