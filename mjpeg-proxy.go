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

import "io"
import "bufio"
import "flag"
import "fmt"
import "strings"
import "strconv"
import "net/http"
import "errors"

/* Sample source stream starts like this:

   HTTP/1.1 200 OK
   Server: nginx/1.2.1
   Date: Mon, 13 Apr 2015 14:02:59 GMT
   Content-Type: multipart/x-mixed-replace;boundary=myboundary
   Transfer-Encoding: chunked
   Connection: keep-alive
   Cache-Control: no-store
   Pragma: no-cache
   Content-Language: en
  
   --myboundary
   Content-Type: image/jpeg
   Content-Length: 36291

   JPEG data...
*/

func chunker(body io.ReadCloser, boundary string, pubChan chan []byte, stopChan chan bool) error {
	fmt.Print("Chunker: starting\n")

	reader := bufio.NewReader(body)
	defer body.Close()

	ChunkLoop:
	for {
		head, size, err := readChunkHeader(reader, boundary)
		if err != nil {
			return err
		}

		data, err := readChunkData(reader, size)
		if err != nil {
			return err
		}

		select {
		case <-stopChan:
			fmt.Print("Chunker: received stop\n")
			break ChunkLoop
		case pubChan <- append(head, data...):
		}

		if size == 0 {
			fmt.Print("Chunker: received final chunk\n")
			break ChunkLoop
		}
	}

	fmt.Print("Chunker: stopping\n")

	return nil
}

func readChunkHeader(reader *bufio.Reader, boundary string) (head []byte, size int, err error) {
	head = make([]byte, 0)
	size = -1
	err = nil

	// read boundary
	var line []byte
	line, err = reader.ReadSlice('\n')
	if err != nil {
		return
	}
	if bl := strings.TrimRight(string(line), "\r\n"); bl != boundary {
		err_str := fmt.Sprintf("Invalid boundary received (%s)", bl) 
		err = errors.New(err_str)
		return
	}
	head = append(head, line...)

	// read header
	for {
		line, err = reader.ReadSlice('\n')
		if err != nil {
			return
		}
		head = append(head, line...)

		// empty line marks end of header
		line_str := strings.TrimRight(string(line), "\r\n")
		if len(line_str) == 0 {
			break
		}

		// find data size
		parts := strings.SplitN(line_str, ": ", 2)
		if strings.EqualFold(parts[0], "Content-Length") {
			var n int
			n, err = strconv.Atoi(string(parts[1]))
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

func readChunkData(reader *bufio.Reader, size int) (buf []byte, err error) {
	buf = make([]byte, size)
	err = nil

	pos := 0
	for pos < size {
		var n int
		n, err = reader.Read(buf[pos:])
		if err != nil {
			return
		}

		pos += n
	}

	return
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

func (pubsub *PubSub) SetHeader(header http.Header) {
	pubsub.header = header
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
		case data := <-pubsub.pubChan:
			pubsub.doPublish(data)

		case sub := <-pubsub.subChan:
			pubsub.doSubscribe(sub)

		case sub := <-pubsub.unsubChan:
			pubsub.doUnsubscribe(sub)
		}
	}
}

func (pubsub *PubSub) doPublish(data []byte) {
	subs := pubsub.subscribers

	for s, _ := range subs {
		select {
		case s.ChunkChannel <- data: // try to send
		default: // or skip this frame
		}
	}
}

func (pubsub *PubSub) doSubscribe(s *Subscriber) {
	pubsub.subscribers[s] = true

	fmt.Printf("PubSub: subscriber %v added (total=%d)\n",
		s, len(pubsub.subscribers))

	if (len(pubsub.subscribers) == 1) {
		if err := pubsub.startPublisher(); err != nil {
			fmt.Printf("PubSub: failed to start publisher (%s)\n", err)
			pubsub.stopSubscribers()
		}
	}
}

func (pubsub *PubSub) stopSubscribers() {
	subs := pubsub.subscribers

	for s, _ := range subs {
		close(s.ChunkChannel)
	}
}

func (pubsub *PubSub) doUnsubscribe(s *Subscriber) {
	delete(pubsub.subscribers, s)

	fmt.Printf("PubSub: subscriber %v removed (total=%d)\n",
		s, len(pubsub.subscribers))

	if (len(pubsub.subscribers) == 0) {
		pubsub.stopPublisher()
	}
}

func (pubsub *PubSub) startPublisher() error {
	fmt.Printf("PubSub: starting publisher for %s\n", pubsub.url)

	req, err := http.NewRequest("GET", pubsub.url, nil)
	if err != nil {
		return err
	}

	if pubsub.username != "" && pubsub.password != "" {
		req.SetBasicAuth(pubsub.username, pubsub.password)
	}

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	body := resp.Body

	if resp.StatusCode != http.StatusOK {
		body.Close()
		err_str := fmt.Sprintf("Request failed (%s)", resp.Status)
		return errors.New(err_str)
	}

	ct := resp.Header.Get("Content-Type")
	prefix := "multipart/x-mixed-replace;boundary="
	if !strings.HasPrefix(ct, prefix) {
		body.Close()
		err_str := fmt.Sprintf("Content-Type is invalid (%s)", ct)
		return errors.New(err_str)
	}
	boundary := "--" + strings.TrimPrefix(ct, prefix)

	pubsub.SetHeader(resp.Header)

	pubsub.pubChan = make(chan []byte)
	pubsub.stopChan = make(chan bool)

	go chunker(resp.Body, boundary, pubsub.pubChan, pubsub.stopChan)

	return nil
}

func (pubsub *PubSub) stopPublisher() {
	fmt.Printf("PubSub: stopping publisher\n")

	if pubsub.stopChan != nil {
		pubsub.stopChan <- true

		close(pubsub.stopChan)
		pubsub.stopChan = nil

		close(pubsub.pubChan)
		pubsub.pubChan = nil
	}
}

type Subscriber struct {
	ChunkChannel chan []byte
}

func NewSubscriber() *Subscriber {
	sub := new(Subscriber)

	sub.ChunkChannel = make(chan []byte)

	return sub
}

func makeHandler(pubsub *PubSub) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		fmt.Printf("Server: client %s connected\n", r.RemoteAddr)

		// prepare response for flushing
		flusher, ok := w.(http.Flusher)
		if !ok {
			fmt.Printf("Server: client %s could not be flushed",
				r.RemoteAddr)
			return
		}

		// subscribe to new chunks
		sub := NewSubscriber()
		pubsub.Subscribe(sub)
		defer pubsub.Unsubscribe(sub)

		headerSet := false
		for {
			// wait for next chunk
			data, ok := <-sub.ChunkChannel
			if (!ok) {
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
				fmt.Printf("Server: client %s failed (%s)\n",
					r.RemoteAddr, err)
				break
			}
		}
	}
}

func main() {
	// check parameters
	sourcePtr := flag.String("source", "http://example.com/img.mjpg", "source mjpg url")
	usernamePtr := flag.String("username", "", "source mjpg username")
	passwordPtr := flag.String("password", "", "source mjpg password")

	bindPtr := flag.String("bind", ":8080", "proxy bind address")
	urlPtr := flag.String("url", "/img.mjpg", "proxy serve url")

	flag.Parse()

	// start pubsub client connector
	pubsub := NewPubSub(*sourcePtr, *usernamePtr, *passwordPtr)
	pubsub.Start()

	// start web server
	http.HandleFunc(*urlPtr, makeHandler(pubsub))
	err := http.ListenAndServe(*bindPtr, nil)
	if err != nil {
		fmt.Printf("Failed to start server: %s\n", err)
	}
}
