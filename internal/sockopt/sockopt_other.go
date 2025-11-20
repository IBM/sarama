//go:build !windows

package sockopt

import "syscall"

const SockoptError = syscall.SO_ERROR

func GetSockoptInt(fd uintptr, opt int) (int, error) {
	return syscall.GetsockoptInt(int(fd), syscall.SOL_SOCKET, opt)
}
