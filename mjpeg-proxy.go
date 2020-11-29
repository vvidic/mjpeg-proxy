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
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"mime"
	"mime/multipart"
	"net"
	"net/http"
	"net/textproto"
	"net/url"
	"os"
	"runtime"
	"strings"
	"time"
)

var stopDelay time.Duration
var tcpSendBuffer int
var trustProxy bool

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
		return nil, fmt.Errorf("uri is not absolute: %s", source)
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
		chunker.closeResponse(resp)
		return fmt.Errorf("request failed: %s", resp.Status)
	}

	boundary, err := getBoundary(resp)
	if err != nil {
		chunker.closeResponse(resp)
		return err
	}

	chunker.resp = resp
	chunker.boundary = boundary
	chunker.stop = make(chan struct{})
	return nil
}

func (chunker *Chunker) closeResponse(resp *http.Response) {
	err := resp.Body.Close()
	if err != nil {
		fmt.Printf("chunker[%s]: body close failed: %s\n", chunker.id, err)
	}
}

func getBoundary(resp *http.Response) (string, error) {
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

func (chunker *Chunker) GetHeader() http.Header {
	return chunker.resp.Header
}

func (chunker *Chunker) Start(pubChan chan []byte) {
	fmt.Printf("chunker[%s]: started\n", chunker.id)

	body := chunker.resp.Body
	defer func() {
		err := body.Close()
		if err != nil {
			fmt.Printf("chunker[%s]: body close failed: %s\n", chunker.id, err)
		}
	}()
	defer close(pubChan)

	var failure error
	mr := multipart.NewReader(body, chunker.boundary)

ChunkLoop:
	for {
		part, err := mr.NextPart()
		if err == io.EOF {
			break ChunkLoop
		}
		if err != nil {
			failure = err
			break ChunkLoop
		}

		data, err := ioutil.ReadAll(part)
		if err != nil {
			failure = err
			break ChunkLoop
		}

		err = part.Close()
		if err != nil {
			failure = err
			break ChunkLoop
		}

		if len(data) == 0 {
			failure = errors.New("received final chunk of size 0")
			break ChunkLoop
		}

		select {
		case <-chunker.stop:
			break ChunkLoop
		case pubChan <- data:
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

func (chunker *Chunker) Started() bool {
	if chunker.stop == nil { // Never started
		return false
	}

	select {
	case <-chunker.stop: // Already stopped
		return false
	default:
		return true // Still running
	}
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

// If frontend proxy is not trusted, return the requests remote address + port.
// If proxy is trusted, return IP + Port if the header IP matched remote address.
// Else, return just the IP address.
func GetClientAddr(r *http.Request) string {
	if !trustProxy {
		return r.RemoteAddr
	}

	remoteHost, remotePort, _ := net.SplitHostPort(r.RemoteAddr)
	parsedHost := net.ParseIP(remoteHost)
	if parsedHost != nil {
		remoteHost = parsedHost.String()
	} else {
		remoteHost = ""
	}
	if len(remotePort) > 0 {
		remotePort = ":" + remotePort
	}

	headerIP := r.Header.Get("x-real-ip")
	parsedHost = net.ParseIP(headerIP)
	if parsedHost != nil {
		headerIP = parsedHost.String()
		if headerIP == remoteHost {
			return headerIP + remotePort
		}
		return headerIP
	}

	hosts := r.Header.Get("x-forwarded-for")
	splitHosts := strings.Split(hosts, ",")
	for _, host := range splitHosts {
		parsedHost = net.ParseIP(host)
		if parsedHost != nil {
			host = parsedHost.String()
			if host == remoteHost {
				return host + remotePort
			}
			return host
		}
	}

	return remoteHost + remotePort
}

func (pubSub *PubSub) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// prepare response for flushing
	flusher, ok := w.(http.Flusher)
	if !ok {
		fmt.Printf("server[%s]: client %s could not be flushed\n",
			pubSub.id, r.RemoteAddr)
		return
	}

	// subscribe to new chunks
	sub := NewSubscriber(GetClientAddr(r))
	pubSub.Subscribe(sub)
	defer pubSub.Unsubscribe(sub)

	mw := multipart.NewWriter(w)
	contentType := fmt.Sprintf("multipart/x-mixed-replace; boundary=%s", mw.Boundary())

	mimeHeader := make(textproto.MIMEHeader)
	mimeHeader.Set("Content-Type", "image/jpeg")

	headersSent := false
	for {
		// wait for next chunk
		data, ok := <-sub.ChunkChannel
		if !ok {
			return
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

	err := mw.Close()
	if err != nil {
		fmt.Printf("server[%s]: mime close failed: %s\n", pubSub.id, err)
	}
}

func startSource(source, username, password, proxyUrl string) error {
	chunker, err := NewChunker(proxyUrl, source, username, password)
	if err != nil {
		return fmt.Errorf("chunker[%s]: create failed: %s", proxyUrl, err)
	}
	pubSub := NewPubSub(proxyUrl, chunker)
	pubSub.Start()

	fmt.Printf("chunker[%s]: serving from %s\n", proxyUrl, source)
	http.Handle(proxyUrl, pubSub)

	return nil
}

type configSource struct {
	Source   string
	Username string
	Password string
	Path     string
}

func loadConfig(filename string) error {
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer func() {
		err := file.Close()
		if err != nil {
			fmt.Printf("config: file close failed for %s: %s\n", file.Name(), err)
		}
	}()

	sources := make([]configSource, 0)
	dec := json.NewDecoder(file)
	err = dec.Decode(&sources)
	if err != nil && err != io.EOF {
		return err
	}

	exists := make(map[string]bool)
	for _, conf := range sources {
		if exists[conf.Path] {
			return fmt.Errorf("duplicate proxy path: %s", conf.Path)
		}

		err = startSource(conf.Source, conf.Username, conf.Password, conf.Path)
		if err != nil {
			return err
		}

		exists[conf.Path] = true
	}

	return nil
}

func connStateEvent(conn net.Conn, event http.ConnState) {
	if event == http.StateActive && tcpSendBuffer > 0 {
		switch c := conn.(type) {
		case *net.TCPConn:
			c.SetWriteBuffer(tcpSendBuffer)
		case *net.UnixConn:
			c.SetWriteBuffer(tcpSendBuffer)
		}
	}
}

func unixListen(path string) (net.Listener, error) {
	fi, err := os.Stat(path)
	if !os.IsNotExist(err) && fi.Mode()&os.ModeSocket != 0 {
		os.Remove(path)
	}

	return net.Listen("unix", path)
}

func listenAndServe(addr string) error {
	var listener net.Listener
	var err error

	if strings.HasPrefix(addr, "unix:") {
		listener, err = unixListen(strings.TrimPrefix(addr, "unix:"))
	} else {
		listener, err = net.Listen("tcp", addr)
	}
	if err != nil {
		return err
	}

	fmt.Printf("server: starting on address %s\n", addr)
	server := &http.Server{
		ConnState: connStateEvent,
	}
	return server.Serve(listener)
}

func main() {
	source := flag.String("source", "http://example.com/img.mjpg", "source uri")
	username := flag.String("username", "", "source uri username")
	password := flag.String("password", "", "source uri password")
	sources := flag.String("sources", "", "JSON configuration file to load sources from")
	bind := flag.String("bind", ":8080", "proxy bind address")
	path := flag.String("path", "/", "proxy serving path")
	maxprocs := flag.Int("maxprocs", 0, "limit number of CPUs used")
	flag.BoolVar(&trustProxy, "trustproxy", false, "trust client IP reporting of proxy")
	flag.DurationVar(&stopDelay, "stopduration", 60*time.Second, "follow source after last client")
	flag.IntVar(&tcpSendBuffer, "sendbuffer", 4096, "limit buffering of frames")
	flag.Parse()

	if *maxprocs > 0 {
		runtime.GOMAXPROCS(*maxprocs)
	}

	var err error
	if *sources != "" {
		err = loadConfig(*sources)
	} else {
		err = startSource(*source, *username, *password, *path)
	}
	if err != nil {
		fmt.Println("config:", err)
		os.Exit(1)
	}

	err = listenAndServe(*bind)
	if err != nil {
		fmt.Println("server:", err)
		os.Exit(1)
	}
}
