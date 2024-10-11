package idempotency

import (
	"sync"
	"time"
)

type InMemory struct {
	processes      map[string]time.Time
	processesMutex sync.RWMutex

	done      map[string]struct{}
	doneMutex sync.RWMutex

	cleanInterval time.Duration
}

func NewInMemory(cleanInterval time.Duration) *InMemory {
	i := &InMemory{
		cleanInterval: cleanInterval,
		processes:     make(map[string]time.Time),
		done:          make(map[string]struct{}),
	}

	go func() {
		ticker := time.NewTicker(cleanInterval)
		for range ticker.C {
			i.clean()
		}
	}()

	return i
}

func (i *InMemory) MarkProcessing(id string) error {
	i.doneMutex.RLock()

	if _, ok := i.done[id]; ok {
		return ErrNotUnique
	}

	i.doneMutex.RUnlock()

	i.clean()
	i.processesMutex.Lock()

	if _, ok := i.processes[id]; ok {
		return ErrNotUnique
	}

	i.processes[id] = time.Now()

	i.processesMutex.Unlock()

	return nil
}

func (i *InMemory) MarkDone(id string) error {
	i.processesMutex.Lock()
	i.doneMutex.Lock()
	delete(i.processes, id)
	i.done[id] = struct{}{}
	i.doneMutex.Unlock()
	i.processesMutex.Unlock()

	return nil
}

func (i *InMemory) clean() {
	i.processesMutex.Lock()

	for id, t := range i.processes {
		if time.Since(t) > i.cleanInterval {
			delete(i.processes, id)
		}
	}

	i.processesMutex.Unlock()
}
