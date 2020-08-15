package buf

import (
	"bytes"
	"sync"
)

//线程安全的内存池
type BufferPool struct {
	sync.Pool
}

func NewBufferPool(bufferSize int) *BufferPool {
	return &BufferPool{
		sync.Pool{
			New: func() interface{} {
				return bytes.NewBuffer(make([]byte, 0, bufferSize))
			},
		},
	}
}

func (bp *BufferPool) Get() *bytes.Buffer {
	return bp.Pool.Get().(*bytes.Buffer)
}

func (bp *BufferPool) Put(b *bytes.Buffer) {
	b.Reset()
	bp.Pool.Put(b)
}