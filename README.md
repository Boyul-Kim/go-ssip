# GO-ssip

An in progress, publish/subscribe (pub/sub) event bus written in Go. The eventual goal is to create a push-based distributed event bus. The main reason for this project is to challenge myself to develop this manually with no code generated from AI (I may use AI as a tool for searching/research or formatting documentation, but that's about it). 

I decided on this project to challenge myself to improve as a software engineer. I find event buses like Kafka to be very interesting. So I'd figure I try to implement my own. After looking into Kafka more, I realize that it is a pull based event bus. For GO-ssip, I'm going to try to create a push based event bus. I think a push based event bus would gel nicely with Go channels.

## Current Features

- **Topic-based pub/sub** — subscribers receive only the topics they subscribe to.
- **Buffered delivery** — each subscriber has its own buffered channel sized by `bufferSize`.
- **Dead letter queue** — events that can't be delivered (full buffer) are captured with a reason.
- **Optional persistence** — `PublishWithStorage` records an event before fanning it out.
- **Concurrency-safe** — internal state is guarded by a `sync.RWMutex`.