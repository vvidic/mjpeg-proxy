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
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"os"
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

type Chunker struct {
	id       string
	source   string
	username string
	password string
	resp     *http.Response
	boundary string
	stop     chan struct{}
}

func NewChunker(id, source, username, password string) (*Chunker, error) {
	chunker := new(Chunker)

	sourceUrl, err := url.Parse(source)
	if err != nil {
		return nil, err
	}
	if !sourceUrl.IsAbs() {
		return nil, fmt.Errorf("url is not absolute: %s", source)
	}

	chunker.id = id
	chunker.source = source
	chunker.username = username
	chunker.password = password

	return chunker, nil
}

func (chunker *Chunker) Connect() error {
	fmt.Printf("chunker[%s]: connecting to %s\n", chunker.id, chunker.source)

	req, err := http.NewRequest("GET", chunker.source, nil)
	if err != nil {
		return err
	}

	if chunker.username != "" && chunker.password != "" {
		req.SetBasicAuth(chunker.username, chunker.password)
	}

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return fmt.Errorf("request failed: %s", resp.Status)
	}

	boundary, err := getBoundary(*resp)
	if err != nil {
		resp.Body.Close()
		return err
	}

	chunker.resp = resp
	chunker.boundary = boundary
	chunker.stop = make(chan struct{})
	return nil
}

func (chunker *Chunker) GetHeader() http.Header {
	return chunker.resp.Header
}

func (chunker *Chunker) Start(pubChan chan []byte) {
	fmt.Printf("chunker[%s]: started\n", chunker.id)

	body := chunker.resp.Body
	reader := bufio.NewReader(body)
	defer func() {
		err := body.Close()
		if err != nil {
			fmt.Printf("chunker[%s]: body close failed: %s\n", chunker.id, err)
		}
	}()
	defer close(pubChan)

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
		case <-chunker.stop:
			break ChunkLoop
		case pubChan <- append(head, data...):
		}

		if size == 0 {
			failure = errors.New("received final chunk of size 0")
			break ChunkLoop
		}
	}

	if failure != nil {
		fmt.Printf("chunker[%s]: failed: %s\n", chunker.id, failure)
	} else {
		fmt.Printf("chunker[%s]: stopped\n", chunker.id)
	}
}

func (chunker *Chunker) Stop() {
	fmt.Printf("chunker[%s]: stopping\n", chunker.id)
	close(chunker.stop)
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
	contentType := resp.Header.Get("Content-Type")
	mediaType, params, err := mime.ParseMediaType(contentType)
	if err != nil {
		return "", err
	}
	if !strings.HasPrefix(mediaType, "multipart/") {
		return "", fmt.Errorf("expected multipart media type: %s", contentType)
	}

	boundary := params["boundary"]
	if boundary == "" {
		return "", fmt.Errorf("boundary not found: %s", contentType)
	}

	return boundary, nil
}

type PubSub struct {
	id          string
	chunker     *Chunker
	pubChan     chan []byte
	subChan     chan *Subscriber
	unsubChan   chan *Subscriber
	subscribers map[*Subscriber]bool
}

func NewPubSub(id string, chunker *Chunker) *PubSub {
	pubsub := new(PubSub)

	pubsub.id = id
	pubsub.chunker = chunker
	pubsub.subChan = make(chan *Subscriber)
	pubsub.unsubChan = make(chan *Subscriber)
	pubsub.subscribers = make(map[*Subscriber]bool)

	return pubsub
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
				pubsub.stopChunker()
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

	fmt.Printf("pubsub[%s]: added subscriber %s (total=%d)\n",
		pubsub.id, s.RemoteAddr, len(pubsub.subscribers))

	if len(pubsub.subscribers) == 1 {
		if err := pubsub.startChunker(); err != nil {
			fmt.Printf("pubsub[%s]: failed to start chunker: %s\n",
				pubsub.id, err)
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

	fmt.Printf("pubsub[%s]: removed subscriber %s (total=%d)\n",
		pubsub.id, s.RemoteAddr, len(pubsub.subscribers))

	if len(pubsub.subscribers) == 0 {
		pubsub.stopChunker()
	}
}

func (pubsub *PubSub) startChunker() error {
	err := pubsub.chunker.Connect()
	if err != nil {
		return err
	}

	pubsub.pubChan = make(chan []byte)
	go pubsub.chunker.Start(pubsub.pubChan)

	return nil
}

func (pubsub *PubSub) stopChunker() {
	if pubsub.pubChan != nil {
		pubsub.chunker.Stop()
	}

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

func (pubsub *PubSub) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// prepare response for flushing
	flusher, ok := w.(http.Flusher)
	if !ok {
		fmt.Printf("server[%s]: client %s could not be flushed\n",
			pubsub.id, r.RemoteAddr)
		return
	}

	// subscribe to new chunks
	sub := NewSubscriber(r.RemoteAddr)
	pubsub.Subscribe(sub)
	defer pubsub.Unsubscribe(sub)

	headersSent := false
	for {
		// wait for next chunk
		data, ok := <-sub.ChunkChannel
		if !ok {
			break
		}

		// send header before first chunk
		if !headersSent {
			header := w.Header()
			for k, vv := range pubsub.chunker.GetHeader() {
				for _, v := range vv {
					header.Add(k, v)
				}
			}
			w.WriteHeader(http.StatusOK)
			headersSent = true
		}

		// send chunk to client
		_, err := w.Write(data)
		flusher.Flush()

		// check for client close
		if err != nil {
			fmt.Printf("server[%s]: client %s failed: %s\n",
				pubsub.id, r.RemoteAddr, err)
			break
		}
	}
}

func startSource(source, username, password, proxyUrl string) error {
	chunker, err := NewChunker(proxyUrl, source, username, password)
	if err != nil {
		return fmt.Errorf("chunker[%s]: create failed: %s", proxyUrl, err)
	}
	pubsub := NewPubSub(proxyUrl, chunker)
	pubsub.Start()

	fmt.Printf("chunker[%s]: serving from %s\n", proxyUrl, source)
	http.Handle(proxyUrl, pubsub)

	return nil
}

type configSource struct {
	Source   string
	Username string
	Password string
	Url      string
}

func loadConfig(config string) error {
	file, err := os.Open(config)
	if err != nil {
		return err
	}
	defer file.Close()

	sources := make([]configSource, 0)
	dec := json.NewDecoder(file)
	err = dec.Decode(&sources)
	if err != nil && err != io.EOF {
		return err
	}

	exists := make(map[string]bool)
	for _, conf := range sources {
		if exists[conf.Url] {
			return fmt.Errorf("duplicate proxy url: %s", conf.Url)
		}

		err = startSource(conf.Source, conf.Username, conf.Password, conf.Url)
		if err != nil {
			return err
		}

		exists[conf.Url] = true
	}

	return nil
}

func main() {
	source := flag.String("source", "http://example.com/img.mjpg", "source mjpg url")
	username := flag.String("username", "", "source mjpg username")
	password := flag.String("password", "", "source mjpg password")
	url := flag.String("url", "/", "proxy serve url")
	config := flag.String("config", "", "JSON configuration file to load")
	bind := flag.String("bind", ":8080", "proxy bind address")
	flag.Parse()

	var err error
	if *config != "" {
		err = loadConfig(*config)
	} else {
		err = startSource(*source, *username, *password, *url)
	}
	if err != nil {
		fmt.Println("config:", err)
		os.Exit(1)
	}

	fmt.Printf("server: starting on address %s\n", *bind)
	err = http.ListenAndServe(*bind, nil)
	if err != nil {
		fmt.Println("server:", err)
		os.Exit(1)
	}
}
