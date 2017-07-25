package storage

import "bytes"

type MessageSet struct{
	offset LogOffset
	buffer []byte
}

func NewMessageSet(offset LogOffset, buffer []byte) MessageSet {
	return MessageSet{
		offset: offset,
		buffer: buffer,
	}
}

type MessageSetEntry struct{
	Location LogOffset
	Header Header
	Payload []byte
}

func (this MessageSet) Scan() []MessageSetEntry {
	entries := make([]MessageSetEntry, 0, 5)

	this.Iterate(func(entry MessageSetEntry) {
		entries = append(entries, entry)
	})

	return entries
}

func (this MessageSet) Iterate(handler func(entry MessageSetEntry)) {
	buffer := bytes.NewBuffer(this.buffer)

	for buffer.Len() > HEADER_SIZE{
		// read the header part
		header := ReadHeader(buffer.Next(HEADER_SIZE))

		// make sure we are at a message start
		if header.Magic != MAGIC_START {
			return
		}

		// make sure we have at least the upcoming value in the buffer
		if int(header.Length) > buffer.Len() {
			return
		}

		// TODO: set full location
		location := LogOffset{ Offset: this.offset.Offset + 1, Page: this.offset.Page }
		payload := buffer.Next(int(header.Length))

		handler(MessageSetEntry{location, header, payload})
	}
}