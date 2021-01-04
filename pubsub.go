/*
 * mjpeg-proxy -- Republish a MJPEG HTTP image stream using a server in Go
 *
 * Copyright (C) 2015-2020, Valentin Vidic
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package main

import (
	"fmt"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"strings"
	"time"
)

type Subscriber struct {
	RemoteAddr   string
	ChunkChannel chan []byte
}

type PubSub struct {
	id          string
	chunker     *Chunker
	pubChan     chan []byte
	subChan     chan *Subscriber
	unsubChan   chan *Subscriber
	subscribers map[*Subscriber]struct{}
	stopTimer   *time.Timer
}

func NewSubscriber(client string) *Subscriber {
	sub := new(Subscriber)

	sub.RemoteAddr = client
	sub.ChunkChannel = make(chan []byte)

	return sub
}

func NewPubSub(id string, chunker *Chunker) *PubSub {
	pubSub := new(PubSub)

	pubSub.id = id
	pubSub.chunker = chunker
	pubSub.subChan = make(chan *Subscriber)
	pubSub.unsubChan = make(chan *Subscriber)
	pubSub.subscribers = make(map[*Subscriber]struct{})
	pubSub.stopTimer = time.NewTimer(0)
	<-pubSub.stopTimer.C

	return pubSub
}

func (pubSub *PubSub) Start() {
	go pubSub.loop()
}

func (pubSub *PubSub) Subscribe(s *Subscriber) {
	pubSub.subChan <- s
}

func (pubSub *PubSub) Unsubscribe(s *Subscriber) {
	pubSub.unsubChan <- s
}

func (pubSub *PubSub) loop() {
	for {
		select {
		case data, ok := <-pubSub.pubChan:
			if ok {
				pubSub.doPublish(data)
			} else {
				pubSub.stopChunker()
				pubSub.stopSubscribers()
			}

		case sub := <-pubSub.subChan:
			pubSub.doSubscribe(sub)

		case sub := <-pubSub.unsubChan:
			pubSub.doUnsubscribe(sub)

		case <-pubSub.stopTimer.C:
			if len(pubSub.subscribers) == 0 {
				pubSub.stopChunker()
			}
		}
	}
}

func (pubSub *PubSub) doPublish(data []byte) {
	subs := pubSub.subscribers

	for s := range subs {
		select {
		case s.ChunkChannel <- data: // try to send
		default: // or skip this frame
		}
	}
}

func (pubSub *PubSub) doSubscribe(s *Subscriber) {
	pubSub.subscribers[s] = struct{}{}

	fmt.Printf("pubsub[%s]: added subscriber %s (total=%d)\n",
		pubSub.id, s.RemoteAddr, len(pubSub.subscribers))

	if len(pubSub.subscribers) == 1 {
		if err := pubSub.startChunker(); err != nil {
			fmt.Printf("pubsub[%s]: failed to start chunker: %s\n",
				pubSub.id, err)
			pubSub.stopSubscribers()
		}
	}
}

func (pubSub *PubSub) stopSubscribers() {
	for s := range pubSub.subscribers {
		close(s.ChunkChannel)
	}
}

func (pubSub *PubSub) doUnsubscribe(s *Subscriber) {
	delete(pubSub.subscribers, s)

	fmt.Printf("pubsub[%s]: removed subscriber %s (total=%d)\n",
		pubSub.id, s.RemoteAddr, len(pubSub.subscribers))

	if len(pubSub.subscribers) == 0 {
		if !pubSub.stopTimer.Stop() {
			select {
			case <-pubSub.stopTimer.C:
			default:
			}
		}
		pubSub.stopTimer.Reset(stopDelay)
	}
}

func (pubSub *PubSub) startChunker() error {
	if pubSub.chunker.Started() {
		return nil
	}

	err := pubSub.chunker.Connect()
	if err != nil {
		return err
	}

	pubSub.pubChan = make(chan []byte)
	go pubSub.chunker.Start(pubSub.pubChan)

	return nil
}

func (pubSub *PubSub) stopChunker() {
	if pubSub.pubChan != nil {
		pubSub.chunker.Stop()
	}

	pubSub.pubChan = nil
}

func clientAddress(r *http.Request) string {
	client := r.RemoteAddr

	if clientHeader != "" {
		header := r.Header.Get(clientHeader)
		hosts := strings.Split(header, ",")
		if hosts[0] != "" {
			client = hosts[0]
		}
	}

	return client
}

func (pubSub *PubSub) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", fmt.Sprintf("%s, %s", http.MethodGet, http.MethodHead))
		http.Error(w, fmt.Sprintf("HTTP method %s not supported", r.Method), http.StatusMethodNotAllowed)
		return
	}

	// prepare response for flushing
	flusher, ok := w.(http.Flusher)
	if !ok {
		fmt.Printf("server[%s]: client %s could not be flushed\n",
			pubSub.id, r.RemoteAddr)
		return
	}

	// subscribe to new chunks
	sub := NewSubscriber(clientAddress(r))
	pubSub.Subscribe(sub)
	defer pubSub.Unsubscribe(sub)

	mw := multipart.NewWriter(w)
	contentType := fmt.Sprintf("multipart/x-mixed-replace; boundary=%s", mw.Boundary())

	mimeHeader := make(textproto.MIMEHeader)
	mimeHeader.Set("Content-Type", "image/jpeg")

	var data []byte
	var chunkOk, headersSent bool

LOOP:
	for {
		// wait for next chunk
		select {
		case data, chunkOk = <-sub.ChunkChannel:
			if !chunkOk {
				break LOOP
			}
		case <-r.Context().Done():
			break LOOP
		}

		// send HTTP header before first chunk
		if !headersSent {
			header := w.Header()
			header.Add("Content-Type", contentType)
			w.WriteHeader(http.StatusOK)
			headersSent = true
		}

		mimeHeader.Set("Content-Size", fmt.Sprintf("%d", len(data)))
		part, err := mw.CreatePart(mimeHeader)
		if err != nil {
			fmt.Printf("server[%s]: part create failed: %s\n", pubSub.id, err)
			return
		}

		// send image to client
		_, err = part.Write(data)
		if err != nil {
			fmt.Printf("server[%s]: part write failed: %s\n", pubSub.id, err)
			return
		}

		flusher.Flush()
	}

	if !headersSent && !chunkOk {
		fmt.Printf("server[%s]: stream failed\n", pubSub.id)
		http.Error(w, "Stream failed", http.StatusServiceUnavailable)
		return
	}

	err := mw.Close()
	if err != nil {
		fmt.Printf("server[%s]: mime close failed: %s\n", pubSub.id, err)
	}
}
