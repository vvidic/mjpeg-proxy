/*
 * mjpeg-proxy -- Republish a MJPEG HTTP image stream using a server in Go
 *
 * Copyright (C) 2015, Valentin Vidic
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
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
)

/* Sample source stream starts like this:

   HTTP/1.1 200 OK
   Content-Type: multipart/x-mixed-replace;boundary=myboundary
   Cache-Control: no-cache
   Pragma: no-cache

   --myboundary
   Content-Type: image/jpeg
   Content-Length: 36291

   JPEG data...
*/

func chunker(body io.ReadCloser, pubChan chan []byte, stopChan chan bool) {
	fmt.Println("chunker: starting")

	reader := bufio.NewReader(body)
	defer func() {
		err := body.Close()
		if err != nil {
			fmt.Println("chunker: body close failed:", err)
		}
	}()
	defer close(pubChan)
	defer close(stopChan)

	var failure error

ChunkLoop:
	for {
		head, size, err := readChunkHeader(reader)
		if err != nil {
			failure = err
			break ChunkLoop
		}

		data, err := readChunkData(reader, size)
		if err != nil {
			failure = err
			break ChunkLoop
		}

		select {
		case <-stopChan:
			break ChunkLoop
		case pubChan <- append(head, data...):
		}

		if size == 0 {
			failure = errors.New("received final chunk")
			break ChunkLoop
		}
	}

	if failure != nil {
		fmt.Println("chunker:", failure)
	}

	fmt.Println("chunker: stopping")
}

func readChunkHeader(reader *bufio.Reader) (head []byte, size int, err error) {
	head = make([]byte, 0)
	size = -1
	err = nil

	// read boundary
	var line []byte
	line, err = reader.ReadSlice('\n')
	if err != nil {
		return
	}

	/* don't check for valid boundary in this function; a lot of webcams
	(such as those by AXIS) seem to provide improper boundaries. */

	head = append(head, line...)

	// read header
	for {
		line, err = reader.ReadSlice('\n')
		if err != nil {
			return
		}
		head = append(head, line...)

		// empty line marks end of header
		lineStr := strings.TrimRight(string(line), "\r\n")
		if len(lineStr) == 0 {
			break
		}

		// find data size
		parts := strings.SplitN(lineStr, ": ", 2)
		if strings.EqualFold(parts[0], "Content-Length") {
			var n int
			n, err = strconv.Atoi(parts[1])
			if err != nil {
				return
			}
			size = n
		}
	}

	if size == -1 {
		err = errors.New("Content-Length chunk header not found")
		return
	}

	return
}

func readChunkData(reader *bufio.Reader, size int) ([]byte, error) {
	buf := make([]byte, size)

	for pos := 0; pos < size; {
		n, err := reader.Read(buf[pos:])
		if err != nil {
			return nil, err
		}

		pos += n
	}

	return buf, nil
}

func getBoundary(resp http.Response) (string, error) {
	ct := strings.Split(resp.Header.Get("Content-Type"), ";")
	fixedCt := ""
	fixedPrefix := "multipart/x-mixed-replace;boundary="

	if len(ct) < 2 || !strings.HasPrefix(ct[0], "multipart/x-mixed-replace") || !strings.HasPrefix(strings.TrimPrefix(ct[1], " "), "boundary=") {
		errStr := fmt.Sprintf("Content-Type is invalid (%s)", strings.Join(ct, ";"))
		return "", errors.New(errStr)
	}
	// Build normalized Content-Type string
	builder := strings.Builder{}
	builder.WriteString(ct[0])
	builder.WriteString(";")
	builder.WriteString(strings.TrimPrefix(ct[1], " "))
	fixedCt = builder.String()

	boundary := "--" + strings.TrimPrefix(fixedCt, fixedPrefix)
	return boundary, nil
}

func connectChunker(url, username, password string) (*http.Response, string, error) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, "", err
	}

	if username != "" && password != "" {
		req.SetBasicAuth(username, password)
	}

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, "", err
	}

	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		errStr := fmt.Sprintf("Request failed (%s)", resp.Status)
		return nil, "", errors.New(errStr)
	}

	boundary, err := getBoundary(*resp)
	if err != nil {
		resp.Body.Close()
		return nil, "", err
	}

	return resp, boundary, nil
}

type PubSub struct {
	url         string
	username    string
	password    string
	pubChan     chan []byte
	stopChan    chan bool
	subChan     chan *Subscriber
	unsubChan   chan *Subscriber
	subscribers map[*Subscriber]bool
	header      http.Header
}

func NewPubSub(url, username, password string) *PubSub {
	pubsub := new(PubSub)

	pubsub.url = url
	pubsub.username = username
	pubsub.password = password

	pubsub.subChan = make(chan *Subscriber)
	pubsub.unsubChan = make(chan *Subscriber)
	pubsub.subscribers = make(map[*Subscriber]bool)

	return pubsub
}

func (pubsub *PubSub) GetHeader() http.Header {
	return pubsub.header
}

func (pubsub *PubSub) Start() {
	go pubsub.loop()
}

func (pubsub *PubSub) Subscribe(s *Subscriber) {
	pubsub.subChan <- s
}

func (pubsub *PubSub) Unsubscribe(s *Subscriber) {
	pubsub.unsubChan <- s
}

func (pubsub *PubSub) loop() {
	for {
		select {
		case data, ok := <-pubsub.pubChan:
			if ok {
				pubsub.doPublish(data)
			} else {
				pubsub.stopChan = nil
				pubsub.stopPublisher()
				pubsub.stopSubscribers()
			}

		case sub := <-pubsub.subChan:
			pubsub.doSubscribe(sub)

		case sub := <-pubsub.unsubChan:
			pubsub.doUnsubscribe(sub)
		}
	}
}

func (pubsub *PubSub) doPublish(data []byte) {
	subs := pubsub.subscribers

	for s := range subs {
		select {
		case s.ChunkChannel <- data: // try to send
		default: // or skip this frame
		}
	}
}

func (pubsub *PubSub) doSubscribe(s *Subscriber) {
	pubsub.subscribers[s] = true

	fmt.Printf("pubsub: added subscriber %s (total=%d)\n",
		s.RemoteAddr, len(pubsub.subscribers))

	if len(pubsub.subscribers) == 1 {
		if err := pubsub.startPublisher(); err != nil {
			fmt.Println("pubsub: failed to start publisher:", err)
			pubsub.stopSubscribers()
		}
	}
}

func (pubsub *PubSub) stopSubscribers() {
	for s := range pubsub.subscribers {
		close(s.ChunkChannel)
	}
}

func (pubsub *PubSub) doUnsubscribe(s *Subscriber) {
	delete(pubsub.subscribers, s)

	fmt.Printf("pubsub: removed subscriber %s (total=%d)\n",
		s.RemoteAddr, len(pubsub.subscribers))

	if len(pubsub.subscribers) == 0 {
		pubsub.stopPublisher()
	}
}

func (pubsub *PubSub) startPublisher() error {
	fmt.Println("pubsub: starting publisher for", pubsub.url)

	resp, _, err := connectChunker(pubsub.url, pubsub.username, pubsub.password)
	if err != nil {
		return err
	}

	pubsub.header = resp.Header
	pubsub.pubChan = make(chan []byte)
	pubsub.stopChan = make(chan bool)

	go chunker(resp.Body, pubsub.pubChan, pubsub.stopChan)

	return nil
}

func (pubsub *PubSub) stopPublisher() {
	if pubsub.stopChan != nil {
		fmt.Println("pubsub: stopping publisher")
		pubsub.stopChan <- true
	}

	pubsub.stopChan = nil
	pubsub.pubChan = nil
}

type Subscriber struct {
	RemoteAddr   string
	ChunkChannel chan []byte
}

func NewSubscriber(client string) *Subscriber {
	sub := new(Subscriber)

	sub.RemoteAddr = client
	sub.ChunkChannel = make(chan []byte)

	return sub
}

func makeHandler(pubsub *PubSub) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		fmt.Printf("server: client %s connected\n", r.RemoteAddr)

		// prepare response for flushing
		flusher, ok := w.(http.Flusher)
		if !ok {
			fmt.Printf("server: client %s could not be flushed",
				r.RemoteAddr)
			return
		}

		// subscribe to new chunks
		sub := NewSubscriber(r.RemoteAddr)
		pubsub.Subscribe(sub)
		defer pubsub.Unsubscribe(sub)

		headerSet := false
		for {
			// wait for next chunk
			data, ok := <-sub.ChunkChannel
			if !ok {
				break
			}

			// set header before first chunk sent
			if !headerSet {
				header := w.Header()
				for k, v := range pubsub.GetHeader() {
					header[k] = v
				}

				headerSet = true
			}

			// send chunk to client
			_, err := w.Write(data)
			flusher.Flush()

			// check for client close
			if err != nil {
				fmt.Printf("server: client %s failed: %s\n",
					r.RemoteAddr, err)
				break
			}
		}
	}
}

func main() {
	// check parameters
	source := flag.String("source", "http://example.com/img.mjpg", "source mjpg url")
	username := flag.String("username", "", "source mjpg username")
	password := flag.String("password", "", "source mjpg password")

	bind := flag.String("bind", ":8080", "proxy bind address")
	url := flag.String("url", "/", "proxy serve url")

	flag.Parse()

	// start pubsub client connector
	pubsub := NewPubSub(*source, *username, *password)
	pubsub.Start()

	// start web server
	fmt.Printf("server: starting on address %s with url %s\n", *bind, *url)
	http.HandleFunc(*url, makeHandler(pubsub))
	err := http.ListenAndServe(*bind, nil)
	if err != nil {
		fmt.Println("server: failed to start:", err)
	}
}
