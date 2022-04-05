package rpcz

const (
	KeepFirstNStreamingMessages = 5
	KeepLastNStreamingMessages  = 5
)

type messageBuffer struct {
	messages     [KeepFirstNStreamingMessages + KeepLastNStreamingMessages]capturedMessage
	messageCount uint64
	huge         bool
}

func (mb *messageBuffer) addMessage(m capturedMessage) {
	if mb.messageCount < KeepFirstNStreamingMessages+KeepLastNStreamingMessages {
		mb.messages[mb.messageCount] = m
	} else {
		mb.messages[KeepFirstNStreamingMessages+((mb.messageCount-KeepFirstNStreamingMessages)%KeepLastNStreamingMessages)] = m
	}
	mb.messageCount++
}

func (mb *messageBuffer) firstMessages() []capturedMessage {
	if mb.messageCount < KeepFirstNStreamingMessages {
		return mb.messages[:mb.messageCount]
	}
	return mb.messages[:KeepFirstNStreamingMessages]
}

func (mb *messageBuffer) lastMessages() []capturedMessage {
	if mb.huge || mb.messageCount <= KeepFirstNStreamingMessages {
		return nil
	}
	if mb.messageCount-KeepFirstNStreamingMessages < KeepLastNStreamingMessages {
		// Between 5-10 messages, no wraparounds happened yet. Simply slice it.
		return mb.messages[KeepFirstNStreamingMessages:mb.messageCount]
	}
	if (mb.messageCount-KeepFirstNStreamingMessages)%KeepLastNStreamingMessages == 0 {
		// We can avoid copying.
		return mb.messages[KeepFirstNStreamingMessages:]
	}
	ret := make([]capturedMessage, KeepLastNStreamingMessages)
	p := copy(ret, mb.messages[KeepFirstNStreamingMessages+((mb.messageCount-KeepFirstNStreamingMessages)%KeepLastNStreamingMessages):])
	copy(ret[p:], mb.messages[KeepLastNStreamingMessages:])
	return ret
}

func (mb *messageBuffer) droppedMessages() uint64 {
	if mb.messageCount <= KeepFirstNStreamingMessages+KeepLastNStreamingMessages {
		return 0
	}
	if mb.huge {
		return mb.messageCount - KeepFirstNStreamingMessages
	}
	return mb.messageCount - KeepFirstNStreamingMessages - KeepLastNStreamingMessages
}
