package rpcz

import (
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

func TestMessageBuffer(t *testing.T) {
	data := make([]capturedMessage, 100)
	for i := 0; 100 > i; i++ {
		data[i] = capturedMessage{message: fmt.Sprint(i)}
	}
	for i := 0; 100 > i; i++ {
		t.Run(fmt.Sprintf("add_upto_%d", i-1), func(t *testing.T) {
			mb := messageBuffer{}
			for j := 0; i > j; j++ {
				if j > 64 {
					mb.huge = true
					mb.messageCount++
					continue
				}
				mb.addMessage(data[j])
			}
			first := i
			if first > KeepFirstNStreamingMessages {
				first = KeepFirstNStreamingMessages
			}
			if diff := cmp.Diff(data[:first], mb.firstMessages(), cmp.AllowUnexported(capturedMessage{})); diff != "" {
				t.Errorf("firstMessages returned incorrect result: %s", diff)
			}
			from := i - KeepLastNStreamingMessages
			till := i
			if i <= KeepFirstNStreamingMessages || mb.huge {
				from = 0
				till = 0
			} else if from < KeepFirstNStreamingMessages {
				from = KeepFirstNStreamingMessages
			}
			if diff := cmp.Diff(data[from:till], mb.lastMessages(), cmp.AllowUnexported(capturedMessage{}), cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("lastMessages returned incorrect result: %s", diff)
			}
			if diff := cmp.Diff(uint64(i-len(mb.firstMessages())-len(mb.lastMessages())), mb.droppedMessages()); diff != "" {
				t.Errorf("droppedMessages returned incorrect result: %s", diff)
			}
		})
	}
}
