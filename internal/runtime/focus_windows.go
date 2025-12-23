//go:build windows
// +build windows

package runtime

import (
	"log"
	"unsafe"

	"golang.org/x/sys/windows"
)

var (
	user32                  = windows.NewLazySystemDLL("user32.dll")
	kernel32                = windows.NewLazySystemDLL("kernel32.dll")
	procSetForegroundWindow = user32.NewProc("SetForegroundWindow")
	procShowWindow          = user32.NewProc("ShowWindow")
	procGetConsoleWindow    = kernel32.NewProc("GetConsoleWindow")
	procSetConsoleTitle     = kernel32.NewProc("SetConsoleTitleW")
	procGetConsoleTitle     = kernel32.NewProc("GetConsoleTitleW")
)

const swRestore = 9

// FocusConsoleWindow tries to bring the current console window to the foreground.
func FocusConsoleWindow() {
	hwnd := getConsoleWindow()
	if hwnd == 0 {
		return
	}

	// Restore in case it is minimized, then ask Windows to focus it.
	_, _, _ = procShowWindow.Call(uintptr(hwnd), uintptr(swRestore))
	if r, _, err := procSetForegroundWindow.Call(uintptr(hwnd)); r == 0 && err != nil {
		log.Printf("%s failed to focus console window: %v", logPrefix, err)
	}
}

// SetConsoleLeaderTitle appends the provided suffix to the current console title.
func SetConsoleLeaderTitle(suffix string) {
	cur := getConsoleTitle()
	newTitle := cur
	if suffix != "" {
		newTitle = cur + " [" + suffix + "]"
	}
	if r, _, err := procSetConsoleTitle.Call(uintptr(unsafe.Pointer(windows.StringToUTF16Ptr(newTitle)))); r == 0 && err != nil {
		log.Printf("%s failed to set console title: %v", logPrefix, err)
	}
}

func getConsoleWindow() windows.Handle {
	hwnd, _, _ := procGetConsoleWindow.Call()
	return windows.Handle(hwnd)
}

func getConsoleTitle() string {
	buf := make([]uint16, 256)
	r, _, _ := procGetConsoleTitle.Call(uintptr(unsafe.Pointer(&buf[0])), uintptr(len(buf)))
	if r == 0 {
		return ""
	}
	return windows.UTF16ToString(buf[:r])
}
