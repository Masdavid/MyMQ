package cmrlimit

import "sync"

type CmrLimit struct {
	sync.Mutex
	countLimit uint16
	curCount  uint16
	sizeLimit  uint32
	curSize   uint32
}

func NewCmrLimit(countLimit uint16, sizeLimit uint32) *CmrLimit {
	return &CmrLimit{
		countLimit: countLimit,
		sizeLimit:  sizeLimit,
		curCount:  0,
		curSize:   0,
	}
}

func (cl *CmrLimit) Update(countLimit uint16, sizeLimit uint32) {
	cl.countLimit = countLimit
	cl.sizeLimit = sizeLimit
}

func (cl *CmrLimit) IsActive() bool {
	return cl.countLimit != 0 || cl.sizeLimit != 0
}

//该函数返回true当增加一个unack消息count成功时，如果超过了prefetchcount，则返回失败
func (cl *CmrLimit) Inc(count uint16, size uint32) bool {
	cl.Lock()
	defer cl.Unlock()

	newCount := cl.curCount + count
	newSize := cl.curSize + size

	if (cl.countLimit == 0 || newCount <= cl.countLimit) && (cl.sizeLimit == 0 || newSize <= cl.sizeLimit) {
		cl.curCount = newCount
		cl.curSize = newSize
		return true
	}

	return false
}

func (cl *CmrLimit) Dec(count uint16, size uint32) {
	cl.Lock()
	defer cl.Unlock()

	if cl.curCount < count {
		cl.curCount = 0
	} else {
		cl.curCount = cl.curCount - count
	}

	if cl.curSize < size {
		cl.curSize = 0
	} else {
		cl.curSize = cl.curSize - size
	}
}

func (cl *CmrLimit) Copy() *CmrLimit {
	cl.Lock()
	defer cl.Unlock()
	return &CmrLimit{
		countLimit: cl.countLimit,
		sizeLimit:  cl.sizeLimit,
		curCount:  cl.curCount,
		curSize:   cl.curSize,
	}
}

func (cl *CmrLimit) getCntLimit() uint16 {
	return cl.countLimit
}

func (cl *CmrLimit) getSizeLimit() uint32 {
	return cl.sizeLimit
}
