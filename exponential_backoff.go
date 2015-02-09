package butteredscones

import (
	"time"
)

type ExponentialBackoff struct {
	Minimum time.Duration
	Maximum time.Duration

	current time.Duration
}

func (b *ExponentialBackoff) Current() time.Duration {
	if b.current == 0 {
		b.current = b.Minimum
	}

	return b.current
}

func (b *ExponentialBackoff) Next() time.Duration {
	if b.current == 0 {
		b.current = b.Minimum
		return b.current
	} else {
		b.current = b.current * 2
		if b.current >= b.Maximum {
			b.current = b.Maximum
		}

		return b.current
	}
}

func (b *ExponentialBackoff) Reset() {
	b.current = 0
}
