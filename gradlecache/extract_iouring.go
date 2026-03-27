//go:build linux

package gradlecache

import (
	"runtime"
	"sync/atomic"
	"syscall"
	"unsafe"

	"github.com/alecthomas/errors"
)

// io_uring syscall numbers (same on x86_64 and arm64).
const (
	sysIoUringSetup    uintptr = 425
	sysIoUringEnter    uintptr = 426
	sysIoUringRegister uintptr = 427

	iringOffSqRing uintptr = 0
	iringOffCqRing uintptr = 0x8000000
	iringOffSqes   uintptr = 0x10000000

	iringOpOpenat = 18
	iringOpWrite  = 23
	iringOpClose  = 19

	iosqeFixedFile = uint8(1 << 0) // sqe->fd is a registered-file-table index
	iosqeIoLink    = uint8(1 << 2) // start of a linked SQE chain

	iringEnterGetevents = uint32(1 << 0)

	iringRegisterFiles = uintptr(2)

	// iringFileIndexAlloc asks the kernel to pick the next free registered-file slot.
	iringFileIndexAlloc = uint32(0xFFFFFFFF) //nolint:unused // documented for reference

	atFdcwd = -100 // AT_FDCWD

	ringEntries = 256             // SQ/CQ size; must be a power of two
	maxInFlight = ringEntries / 3 // 3 SQEs per file (openat+write+close)
)

// ioUringParams is the 120-byte struct passed to io_uring_setup.
type ioUringParams struct {
	SqEntries    uint32
	CqEntries    uint32
	Flags        uint32
	SqThreadCpu  uint32
	SqThreadIdle uint32
	Features     uint32
	WqFd         uint32
	Resv         [3]uint32
	SqOff        sqRingOffsets // 40 bytes starting at offset 40
	CqOff        cqRingOffsets // 40 bytes starting at offset 80
} // total: 120 bytes

type sqRingOffsets struct {
	Head        uint32
	Tail        uint32
	RingMask    uint32
	RingEntries uint32
	Flags       uint32
	Dropped     uint32
	Array       uint32
	Resv1       uint32
	UserAddr    uint64
} // 40 bytes

type cqRingOffsets struct {
	Head        uint32
	Tail        uint32
	RingMask    uint32
	RingEntries uint32
	Overflow    uint32
	Cqes        uint32
	Flags       uint32
	Resv1       uint32
	UserAddr    uint64
} // 40 bytes

// ioUringSqe is the 64-byte Submission Queue Entry.
type ioUringSqe struct {
	Opcode      uint8
	Flags       uint8 // IOSQE_* flags
	Ioprio      uint16
	Fd          int32  // openat: dirfd; write+close-with-fixed: slot index
	Off         uint64 // write: file offset (use 0)
	Addr        uint64 // openat: *filename; write: *data
	Len         uint32 // openat: permission mode; write: data length
	OpenFlags   uint32 // openat: O_WRONLY|O_CREAT|O_TRUNC; others: 0
	UserData    uint64 // caller tag: (slot<<2)|opKind
	BufIndex    uint16
	Personality uint16
	FileIndex   int32 // openat: slot+1 to install result; close: slot+1 to remove
	Addr3       uint64
	Pad         uint64
} // 64 bytes

// ioUringCqe is the 16-byte Completion Queue Entry.
type ioUringCqe struct {
	UserData uint64
	Res      int32 // return value (negative = errno)
	Flags    uint32
} // 16 bytes

// Compile-time size checks — if any struct has the wrong size, the build fails.
var _ [64]struct{} = [unsafe.Sizeof(ioUringSqe{})]struct{}{}
var _ [16]struct{} = [unsafe.Sizeof(ioUringCqe{})]struct{}{}
var _ [120]struct{} = [unsafe.Sizeof(ioUringParams{})]struct{}{}

const (
	opKindOpenat = 0
	opKindWrite  = 1
	opKindClose  = 2
)

func makeUserData(slot int, kind int) uint64 { return uint64(slot<<2) | uint64(kind) }
func slotFromUserData(ud uint64) int         { return int(ud >> 2) }
func kindFromUserData(ud uint64) int         { return int(ud & 3) }

// iouRing wraps a single io_uring instance with mmap'd SQ/CQ rings and a
// registered file table for linked openat→write→close chains.
type iouRing struct {
	fd int

	// SQ mmap
	sqRingBuf []byte
	sqesBuf   []byte
	// CQ mmap
	cqRingBuf []byte

	// Pointers into mmap'd memory (derived from params offsets after setup).
	sqHead  *uint32 // kernel writes, we read (atomic)
	sqTail  *uint32 // we write (atomic), kernel reads
	sqMask  *uint32
	sqArray *uint32 // base of the uint32 SQ index array

	cqHead *uint32 // we write (atomic), kernel reads
	cqTail *uint32 // kernel writes, we read (atomic)
	cqMask *uint32
	cqes   unsafe.Pointer // base of the CQE array

	sqes unsafe.Pointer // base of the SQE array

	// Registered file table. -1 = empty slot.
	regFiles []int32 // len = maxInFlight
}

func newIouRing() (*iouRing, error) {
	var params ioUringParams
	fd, _, errno := syscall.Syscall(sysIoUringSetup, uintptr(ringEntries),
		uintptr(unsafe.Pointer(&params)), 0)
	if errno != 0 {
		return nil, errors.Errorf("io_uring_setup: %w", errno)
	}
	r := &iouRing{fd: int(fd)}

	// Compute mmap sizes from the offsets returned by the kernel.
	sqRingSize := uintptr(params.SqOff.Array) + uintptr(params.SqEntries)*4
	sqesSize := uintptr(params.SqEntries) * 64
	cqRingSize := uintptr(params.CqOff.Cqes) + uintptr(params.CqEntries)*16

	var err error
	r.sqRingBuf, err = syscall.Mmap(int(fd), int64(iringOffSqRing), int(sqRingSize),
		syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED|syscall.MAP_POPULATE)
	if err != nil {
		syscall.Close(int(fd)) //nolint:errcheck
		return nil, errors.Errorf("mmap sq ring: %w", err)
	}
	r.sqesBuf, err = syscall.Mmap(int(fd), int64(iringOffSqes), int(sqesSize),
		syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED|syscall.MAP_POPULATE)
	if err != nil {
		r.close()
		return nil, errors.Errorf("mmap sqes: %w", err)
	}
	r.cqRingBuf, err = syscall.Mmap(int(fd), int64(iringOffCqRing), int(cqRingSize),
		syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED|syscall.MAP_POPULATE)
	if err != nil {
		r.close()
		return nil, errors.Errorf("mmap cq ring: %w", err)
	}

	// Derive typed pointers from mmap offsets.
	sqBase := unsafe.Pointer(&r.sqRingBuf[0])
	r.sqHead = (*uint32)(unsafe.Add(sqBase, params.SqOff.Head))
	r.sqTail = (*uint32)(unsafe.Add(sqBase, params.SqOff.Tail))
	r.sqMask = (*uint32)(unsafe.Add(sqBase, params.SqOff.RingMask))
	r.sqArray = (*uint32)(unsafe.Add(sqBase, params.SqOff.Array))

	cqBase := unsafe.Pointer(&r.cqRingBuf[0])
	r.cqHead = (*uint32)(unsafe.Add(cqBase, params.CqOff.Head))
	r.cqTail = (*uint32)(unsafe.Add(cqBase, params.CqOff.Tail))
	r.cqMask = (*uint32)(unsafe.Add(cqBase, params.CqOff.RingMask))
	r.cqes = unsafe.Add(cqBase, params.CqOff.Cqes)

	r.sqes = unsafe.Pointer(&r.sqesBuf[0])

	// Register a fixed-file table (all slots empty = -1).
	r.regFiles = make([]int32, maxInFlight)
	for i := range r.regFiles {
		r.regFiles[i] = -1
	}
	_, _, errno = syscall.Syscall6(sysIoUringRegister, uintptr(r.fd),
		iringRegisterFiles,
		uintptr(unsafe.Pointer(&r.regFiles[0])),
		uintptr(len(r.regFiles)), 0, 0)
	if errno != 0 {
		r.close()
		return nil, errors.Errorf("io_uring_register files: %w", errno)
	}

	return r, nil
}

func (r *iouRing) close() {
	if r.sqRingBuf != nil {
		syscall.Munmap(r.sqRingBuf) //nolint:errcheck
	}
	if r.sqesBuf != nil {
		syscall.Munmap(r.sqesBuf) //nolint:errcheck
	}
	if r.cqRingBuf != nil {
		syscall.Munmap(r.cqRingBuf) //nolint:errcheck
	}
	syscall.Close(r.fd) //nolint:errcheck
}

// sqeAt returns a pointer to the SQE at position idx (wrapping by ring mask).
func (r *iouRing) sqeAt(idx uint32) *ioUringSqe {
	mask := atomic.LoadUint32(r.sqMask)
	return (*ioUringSqe)(unsafe.Add(r.sqes, uintptr(idx&mask)*64))
}

// cqeAt returns a pointer to the CQE at position idx.
func (r *iouRing) cqeAt(idx uint32) *ioUringCqe {
	mask := atomic.LoadUint32(r.cqMask)
	return (*ioUringCqe)(unsafe.Add(r.cqes, uintptr(idx&mask)*16))
}

// getSqTail returns the current SQ tail index.
func (r *iouRing) getSqTail() uint32 {
	return atomic.LoadUint32(r.sqTail)
}

// flushSq advances the SQ tail by n and calls io_uring_enter to submit.
func (r *iouRing) flushSq(n uint32) error {
	tail := atomic.LoadUint32(r.sqTail)
	mask := atomic.LoadUint32(r.sqMask)
	// Fill the SQ index array — maps ring-position to SQE index.
	for i := uint32(0); i < n; i++ {
		slot := (tail + i) & mask
		*(*uint32)(unsafe.Add(unsafe.Pointer(r.sqArray), uintptr(slot)*4)) = slot
	}
	atomic.StoreUint32(r.sqTail, tail+n)
	_, _, errno := syscall.Syscall6(sysIoUringEnter,
		uintptr(r.fd), uintptr(n), 0, 0, 0, 0)
	if errno != 0 {
		return errors.Errorf("io_uring_enter submit: %w", errno)
	}
	return nil
}

// waitCqe blocks until at least one CQE is available and returns the head index.
func (r *iouRing) waitCqe() (uint32, error) {
	for {
		head := atomic.LoadUint32(r.cqHead)
		tail := atomic.LoadUint32(r.cqTail)
		if tail != head {
			return head, nil
		}
		_, _, errno := syscall.Syscall6(sysIoUringEnter,
			uintptr(r.fd), 0, 1, uintptr(iringEnterGetevents), 0, 0)
		if errno != 0 && errno != syscall.EINTR {
			return 0, errors.Errorf("io_uring_enter wait: %w", errno)
		}
	}
}

// advanceCq marks one CQE as consumed.
func (r *iouRing) advanceCq() {
	atomic.StoreUint32(r.cqHead, atomic.LoadUint32(r.cqHead)+1)
}

// extractTarIoUring consumes writeJobs and creates files using io_uring linked
// SQE chains (openat → write → close). A single goroutine submits and reaps
// all operations; backpressure is provided by a slot pool of maxInFlight entries.
func extractTarIoUring(jobs <-chan writeJob) error {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	ring, err := newIouRing()
	if err != nil {
		return err
	}
	defer ring.close()

	// slotPool provides backpressure — only maxInFlight chains in progress.
	slotPool := make(chan int, maxInFlight)
	for i := range maxInFlight {
		slotPool <- i
	}

	// Pin filename pointers and data slices until their CQEs arrive.
	filenamePtrs := make([]*byte, maxInFlight)
	pendingData := make([][]byte, maxInFlight)

	var firstErr error
	inFlight := 0

	reap := func() error {
		head, err := ring.waitCqe()
		if err != nil {
			return err
		}
		cqe := ring.cqeAt(head)
		ring.advanceCq()

		kind := kindFromUserData(cqe.UserData)
		slot := slotFromUserData(cqe.UserData)

		switch kind {
		case opKindOpenat:
			// Kernel has consumed the filename pointer.
			filenamePtrs[slot] = nil
			if cqe.Res < 0 && firstErr == nil {
				firstErr = errors.Errorf("io_uring openat slot %d: errno %d", slot, -cqe.Res)
			}
		case opKindWrite:
			// Release the data reference.
			pendingData[slot] = nil
			if cqe.Res < 0 && cqe.Res != -int32(syscall.ECANCELED) && firstErr == nil {
				firstErr = errors.Errorf("io_uring write slot %d: errno %d", slot, -cqe.Res)
			}
		case opKindClose:
			if cqe.Res < 0 && cqe.Res != -int32(syscall.ECANCELED) && firstErr == nil {
				firstErr = errors.Errorf("io_uring close slot %d: errno %d", slot, -cqe.Res)
			}
			// Chain complete — release slot.
			slotPool <- slot
			inFlight--
		}
		return nil
	}

	for job := range jobs {
		// Acquire a slot. Interleave reaping while waiting to avoid deadlock.
		var slot int
		for {
			select {
			case slot = <-slotPool:
				goto gotSlot
			default:
			}
			if err := reap(); err != nil {
				return err
			}
		}
	gotSlot:

		// Convert filename to null-terminated bytes and pin it.
		fnBytes, err := syscall.BytePtrFromString(job.target)
		if err != nil {
			slotPool <- slot
			return errors.Errorf("bad filename %s: %w", job.target, err)
		}
		filenamePtrs[slot] = fnBytes
		pendingData[slot] = job.data
		mode := uint32(job.mode.Perm())

		tail := ring.getSqTail()
		hasData := len(job.data) > 0

		// SQE 0 — OPENAT: create the file, install result at fixed-file slot.
		sqe0 := ring.sqeAt(tail)
		*sqe0 = ioUringSqe{
			Opcode:    iringOpOpenat,
			Flags:     iosqeIoLink,
			Fd:        atFdcwd,
			Addr:      uint64(uintptr(unsafe.Pointer(fnBytes))),
			Len:       mode,
			OpenFlags: syscall.O_WRONLY | syscall.O_CREAT | syscall.O_TRUNC,
			FileIndex: int32(slot + 1),
			UserData:  makeUserData(slot, opKindOpenat),
		}

		numSqes := uint32(2)
		if hasData {
			// SQE 1 — WRITE: write data via the registered file slot.
			sqe1 := ring.sqeAt(tail + 1)
			*sqe1 = ioUringSqe{
				Opcode:   iringOpWrite,
				Flags:    iosqeFixedFile | iosqeIoLink,
				Fd:       int32(slot),
				Addr:     uint64(uintptr(unsafe.Pointer(&job.data[0]))),
				Len:      uint32(len(job.data)),
				UserData: makeUserData(slot, opKindWrite),
			}

			// SQE 2 — CLOSE: remove the registered file.
			sqe2 := ring.sqeAt(tail + 2)
			*sqe2 = ioUringSqe{
				Opcode:    iringOpClose,
				FileIndex: int32(slot + 1),
				UserData:  makeUserData(slot, opKindClose),
			}
			numSqes = 3
		} else {
			// Empty file: skip WRITE, just OPENAT (linked) → CLOSE.
			// Still need a WRITE CQE for the reaper — submit a no-op write
			// by going straight to CLOSE instead.
			sqe1 := ring.sqeAt(tail + 1)
			*sqe1 = ioUringSqe{
				Opcode:    iringOpClose,
				FileIndex: int32(slot + 1),
				UserData:  makeUserData(slot, opKindClose),
			}
			// No write CQE will arrive; clear pendingData now.
			pendingData[slot] = nil
		}

		if err := ring.flushSq(numSqes); err != nil {
			return err
		}
		// Keep GC from collecting pinned data before the kernel reads it.
		runtime.KeepAlive(fnBytes)
		runtime.KeepAlive(job.data)
		inFlight++

		// Drain any available CQEs without blocking.
		for {
			head := atomic.LoadUint32(ring.cqHead)
			tail2 := atomic.LoadUint32(ring.cqTail)
			if head == tail2 {
				break
			}
			if err := reap(); err != nil {
				return err
			}
		}

		if firstErr != nil {
			return firstErr
		}
	}

	// Drain remaining in-flight chains.
	for inFlight > 0 {
		if err := reap(); err != nil {
			return err
		}
	}
	return firstErr
}
