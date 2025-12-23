//go:build !windows
// +build !windows

package runtime

// FocusConsoleWindow is a no-op on non-Windows platforms.
func FocusConsoleWindow() {}

// SetConsoleLeaderTitle is a no-op on non-Windows platforms.
func SetConsoleLeaderTitle(string) {}
