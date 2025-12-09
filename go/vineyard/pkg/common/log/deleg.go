/*
Copyright 2018 The Kubernetes Authors.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package log

import (
	"sync"

	"github.com/go-logr/logr"
)

// loggerPromise knows how to populate a concrete logr.LogSink
// with options, given an actual base logger later on down the line.
type loggerPromise struct {
	sink          *DelegatingLogSink
	childPromises []*loggerPromise
	promisesLock  sync.Mutex

	name *string
	tags []any
}

func (p *loggerPromise) WithName(l *DelegatingLogSink, name string) *loggerPromise {
	res := &loggerPromise{
		sink:         l,
		name:         &name,
		promisesLock: sync.Mutex{},
	}

	p.promisesLock.Lock()
	defer p.promisesLock.Unlock()
	p.childPromises = append(p.childPromises, res)
	return res
}

func (p *loggerPromise) WithValues(l *DelegatingLogSink, tags ...any) *loggerPromise {
	res := &loggerPromise{
		sink:         l,
		tags:         tags,
		promisesLock: sync.Mutex{},
	}

	p.promisesLock.Lock()
	defer p.promisesLock.Unlock()
	p.childPromises = append(p.childPromises, res)
	return res
}

// Fulfill instantiates the LogSink with the provided sink.
func (p *loggerPromise) Fulfill(parentLogSink logr.LogSink) {
	sink := parentLogSink
	if p.name != nil {
		sink = sink.WithName(*p.name)
	}

	if p.tags != nil {
		sink = sink.WithValues(p.tags...)
	}

	p.sink.lock.Lock()
	p.sink.sink = sink
	p.sink.promise = nil
	p.sink.lock.Unlock()

	for _, childPromise := range p.childPromises {
		childPromise.Fulfill(sink)
	}
}

// DelegatingLogSink is a logsink that delegates to another logr.LogSink.
type DelegatingLogSink struct {
	lock    sync.RWMutex
	sink    logr.LogSink
	promise *loggerPromise
}

// Ensure DelegatingLogSink implements LogSink
var _ logr.LogSink = &DelegatingLogSink{}

func (l *DelegatingLogSink) Init(info logr.RuntimeInfo) {
	l.lock.Lock()
	defer l.lock.Unlock()
	// We need to pass Init down if the sink is already set?
	// But usually Init is called by logr.New.
	// If we are delegated, the underlying sink might already be inited.
	// We can store info if needed, but for now we just delegate access.
}

func (l *DelegatingLogSink) Enabled(level int) bool {
	l.lock.RLock()
	defer l.lock.RUnlock()
	return l.sink.Enabled(level)
}

func (l *DelegatingLogSink) Info(level int, msg string, keysAndValues ...any) {
	l.lock.RLock()
	defer l.lock.RUnlock()
	l.sink.Info(level, msg, keysAndValues...)
}

func (l *DelegatingLogSink) Error(err error, msg string, keysAndValues ...any) {
	l.lock.RLock()
	defer l.lock.RUnlock()
	l.sink.Error(err, msg, keysAndValues...)
}

func (l *DelegatingLogSink) WithName(name string) logr.LogSink {
	l.lock.RLock()
	defer l.lock.RUnlock()

	if l.promise == nil {
		return l.sink.WithName(name)
	}

	res := &DelegatingLogSink{sink: l.sink}
	promise := l.promise.WithName(res, name)
	res.promise = promise

	return res
}

func (l *DelegatingLogSink) WithValues(tags ...any) logr.LogSink {
	l.lock.RLock()
	defer l.lock.RUnlock()

	if l.promise == nil {
		return l.sink.WithValues(tags...)
	}

	res := &DelegatingLogSink{sink: l.sink}
	promise := l.promise.WithValues(res, tags...)
	res.promise = promise

	return res
}

// Fulfill switches the logger over to use the actual logger
// provided, instead of the temporary initial one.
func (l *DelegatingLogSink) Fulfill(actual logr.Logger) {
	if l.promise != nil {
		l.promise.Fulfill(actual.GetSink())
	}
}

// NewDelegatingLogSink constructs a new DelegatingLogSink which uses
// the given logger (sink) before its promise is fulfilled.
func NewDelegatingLogSink(initial logr.Logger) *DelegatingLogSink {
	l := &DelegatingLogSink{
		sink:    initial.GetSink(),
		promise: &loggerPromise{promisesLock: sync.Mutex{}},
	}
	l.promise.sink = l
	return l
}
