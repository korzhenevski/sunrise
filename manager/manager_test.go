package manager

import (
	"fmt"
	"testing"
)

func ExampleExtractStreamTitle() {
	fmt.Println(ExtractStreamTitle("StreamTitle='Testing'"))
	// Output: Testing
	fmt.Println(ExtractStreamTitle("StreamTitle=''"))
	// Output:
	fmt.Println(ExtractStreamTitle(""))
	// Output:
}

func BenchmarkExtractStreamTitle(b *testing.B) {
	for i := 0; i < b.N; i++ {
		ExtractStreamTitle("StreamTitle='Testing'")
	}
}
