package main

import (
	"fmt"
	"os"

	"github.com/fatih/color"
	"github.com/shirou/gopsutil/v4/cpu"
	"github.com/shirou/gopsutil/v4/disk"
	"github.com/shirou/gopsutil/v4/host"
	"github.com/shirou/gopsutil/v4/mem"
)

var (
	pastelColor = color.RGB(95, 175, 255)
	grayColor   = color.RGB(138, 138, 138)
	lightGreen  = color.RGB(197, 255, 167)
)

func printLogo(version string) {
	_, _ = pastelColor.Print(logo)
	_, _ = pastelColor.Printf("\nPirinDB Version %s\n", version)
}

func printSystemInfo() {
	arch, _ := host.Info()
	cores, _ := cpu.Counts(false)
	threads, _ := cpu.Counts(true)
	vmem, _ := mem.VirtualMemory()
	cwd, _ := os.Getwd()
	usage, _ := disk.Usage(cwd)

	fmt.Printf("\n%s %s | %s %s | %s %s \n",
		grayColor.Sprint("Arch:"), pastelColor.Sprint(arch.KernelArch),
		grayColor.Sprint("Cores:"), pastelColor.Sprint(cores),
		grayColor.Sprint("Threads:"), pastelColor.Sprint(threads))

	fmt.Printf("%s %s total / %s free\n",
		grayColor.Sprint("Mem: "), pastelColor.Sprintf("%.1fGB", float64(vmem.Total)/1e9),
		lightGreen.Sprintf("%.1fGB", float64(vmem.Free)/1e9))

	fmt.Printf("%s %s total / %s free @ %s\n\n",
		grayColor.Sprint("Disk:"), pastelColor.Sprintf("%.1fGB", float64(usage.Total)/1e9),
		lightGreen.Sprintf("%.1fGB", float64(usage.Free)/1e9), pastelColor.Sprint(cwd))
}
