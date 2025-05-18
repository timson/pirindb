package main

import "fmt"

func uint64PtrToHex(p *uint64) string {
	if p == nil {
		return ""
	}
	return fmt.Sprintf("%x", *p)
}
