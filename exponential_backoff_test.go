package butteredscones

import (
	"testing"
	"time"
)

func TestExponentialBackoff(t *testing.T) {
	backoff := &ExponentialBackoff{
		Minimum: 1 * time.Second,
		Maximum: 10 * time.Second,
	}

	dur := backoff.Next()
	if dur != 1*time.Second {
		t.Fatalf("Expected %q, but got %q", 1*time.Second, dur)
	}

	dur = backoff.Next()
	if dur != 2*time.Second {
		t.Fatalf("Expected %q, but got %q", 2*time.Second, dur)
	}

	dur = backoff.Next()
	if dur != 4*time.Second {
		t.Fatalf("Expected %q, but got %q", 4*time.Second, dur)
	}

	dur = backoff.Next()
	if dur != 8*time.Second {
		t.Fatalf("Expected %q, but got %q", 8*time.Second, dur)
	}

	// Maximum
	dur = backoff.Next()
	if dur != 10*time.Second {
		t.Fatalf("Expected %q, but got %q", 10*time.Second, dur)
	}

	// Maximum again
	dur = backoff.Next()
	if dur != 10*time.Second {
		t.Fatalf("Expected %q, but got %q", 10*time.Second, dur)
	}

	// Reset
	backoff.Reset()

	dur = backoff.Next()
	if dur != 1*time.Second {
		t.Fatalf("Expected %q, but got %q", 1*time.Second, dur)
	}
}
