package gradlecache

import (
	"os"
	"syscall"
	"time"
)

// fileAtime returns the access time of the file at path.
func fileAtime(path string) time.Time {
	var st syscall.Stat_t
	if err := syscall.Stat(path, &st); err != nil {
		// Fall back to os.Stat which doesn't give atime directly.
		if fi, err := os.Stat(path); err == nil {
			return fi.ModTime()
		}
		return time.Time{}
	}
	return time.Unix(st.Atim.Sec, st.Atim.Nsec)
}
