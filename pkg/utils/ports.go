package utils

import (
	"fmt"
	"math/rand/v2"
	"net"
)

func GenerateAvailablePort() (int, error) {
	for i := 0; i < 100; i++ {
		port := int(rand.Int32N(65535-1024+1) + 1024)
		if isPortAvailable(port) {
			return port, nil
		}
	}
	return 0, fmt.Errorf("no available port found")
}

func isPortAvailable(port int) bool {
	conn, err := net.Dial("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return true
	}
	_ = conn.Close()
	return false
}
