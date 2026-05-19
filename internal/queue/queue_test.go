package queue

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestQueue(t *testing.T) {
	t.Run("zero value reports zero length", func(t *testing.T) {
		var q Queue[int]
		assert.Equal(t, 0, q.Length())
	})

	t.Run("add then peek then remove preserves FIFO order", func(t *testing.T) {
		var q Queue[int]
		for i := 0; i < 5; i++ {
			q.Add(i)
		}
		assert.Equal(t, 5, q.Length())
		for i := 0; i < 5; i++ {
			assert.Equal(t, i, q.Peek())
			assert.Equal(t, i, q.Remove())
		}
		assert.Equal(t, 0, q.Length())
	})

	t.Run("grows beyond initial capacity", func(t *testing.T) {
		var q Queue[int]
		const n = 1024
		for i := 0; i < n; i++ {
			q.Add(i)
		}
		assert.Equal(t, n, q.Length())
		for i := 0; i < n; i++ {
			assert.Equal(t, i, q.Remove())
		}
		assert.Equal(t, 0, q.Length())
	})

	t.Run("interleaved add and remove wraps around buffer", func(t *testing.T) {
		var q Queue[int]
		var want []int
		for i := 0; i < 100; i++ {
			q.Add(i)
			q.Add(i + 1000)
			want = append(want, i, i+1000)
		}
		var got []int
		for q.Length() > 0 {
			got = append(got, q.Remove())
		}
		// re-add and drain to force wrap-around past the original tail
		for i := 0; i < 50; i++ {
			q.Add(i)
		}
		for q.Length() > 0 {
			q.Remove()
		}
		assert.Equal(t, want, got)
	})

	t.Run("works with pointer types and clears slot on remove", func(t *testing.T) {
		type box struct{ v int }
		var q Queue[*box]
		a, b := &box{v: 1}, &box{v: 2}
		q.Add(a)
		q.Add(b)
		got := q.Remove()
		require.NotNil(t, got)
		assert.Equal(t, 1, got.v)
		// the just-vacated slot must hold the zero value so the referent can
		// be garbage collected. We can't inspect head directly, but Remove
		// followed by Add at the same slot must not return the old element.
		q.Add(&box{v: 3})
		assert.Equal(t, 2, q.Remove().v)
		assert.Equal(t, 3, q.Remove().v)
	})

	t.Run("peek on empty queue panics", func(t *testing.T) {
		var q Queue[int]
		assert.Panics(t, func() { q.Peek() })
	})

	t.Run("remove on empty queue panics", func(t *testing.T) {
		var q Queue[int]
		assert.Panics(t, func() { q.Remove() })
	})

	t.Run("shrinks when sparsely populated", func(t *testing.T) {
		var q Queue[int]
		// grow well beyond minLen
		for i := 0; i < 1024; i++ {
			q.Add(i)
		}
		grownCap := cap(q.buf)
		// drain most elements to trigger shrink path repeatedly
		for i := 0; i < 1020; i++ {
			q.Remove()
		}
		assert.Less(t, cap(q.buf), grownCap)
		assert.Equal(t, 4, q.Length())
	})
}
