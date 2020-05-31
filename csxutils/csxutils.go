package csxutils

// Unpstr return string from pointer
func Unpstr(ref *string) string {
	if ref != nil {
		return *ref
	}
	return ""
}
