//go:build windows

package sockopt

const SockoptError = 0

func GetSockoptInt(fd uintptr, opt int) (int, error) {
	return 0, nil
}
