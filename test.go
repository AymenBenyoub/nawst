package main

import (
	"fmt"
	"github.com/shirou/gopsutil/v4/disk"
	"io/ioutil"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

func main() {
	b, _ := ioutil.ReadFile("/proc/meminfo")
	lines := strings.Split(string(b), "\n")
	for _, ln := range lines {
		if strings.HasPrefix(ln, "MemTotal:") {
			fields := strings.Fields(ln)
			kb, _ := strconv.ParseInt(fields[1], 10, 64)
			fmt.Printf("MemTotal KB: %d, GB: %d\n", kb, kb/(1024*1024))
		}
	}

	u, err := disk.Usage("/")
	if err == nil {
		fmt.Printf("Disk Total: %d GB, Used: %f %%\n", u.Total/(1024*1024*1024), u.UsedPercent)
	} else {
		fmt.Println("Disk err:", err)
	}

	u2, err2 := disk.Usage("/home")
	if err2 == nil {
		fmt.Printf("Disk /home Total: %d GB, Used: %f %%\n", u2.Total/(1024*1024*1024), u2.UsedPercent)
	}

	ifaces, _ := net.Interfaces()
	for _, iface := range ifaces {
		speedPath := filepath.Join("/sys/class/net", iface.Name, "speed")
		sb, _ := os.ReadFile(speedPath)
		fmt.Printf("Net %s speed: %s\n", iface.Name, strings.TrimSpace(string(sb)))
	}
}
