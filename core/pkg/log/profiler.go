package log

import "time"

type Profiler struct {
	profiles map[string]time.Duration
	starts   map[string]time.Time
}

func NewProfiler() *Profiler {
	return &Profiler{
		profiles: map[string]time.Duration{},
		starts:   map[string]time.Time{},
	}
}

func (p *Profiler) Start(name string) {
	if p == nil {
		return
	}
	p.starts[name] = time.Now()
}

func (p *Profiler) Stop(name string) time.Duration {
	if p == nil {
		return 0
	}
	if start, ok := p.starts[name]; ok {
		elapsed := time.Since(start)
		p.profiles[name] += elapsed
		return elapsed
	}
	return 0
}

func (p *Profiler) Log(name string) {
	if p == nil {
		return
	}
	Profilef("%s: %s", p.profiles[name], name)
}

func (p *Profiler) LogAll() {
	if p == nil {
		return
	}

	// Print profiles, largest to smallest. (Inefficient, but shouldn't matter.)
	print := map[string]time.Duration{}
	for name, value := range p.profiles {
		print[name] = value
	}
	for len(print) > 0 {
		largest := ""
		for name := range print {
			if print[name] > print[largest] {
				largest = name
			}
		}
		Profilef("%s: %s", print[largest], largest)
		delete(print, largest)
	}
}
