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
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"mime"
	"mime/multipart"
	"net/http"
	"net/url"
	"strings"
	"time"
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
	rate     float64
}

func NewChunker(id, source, username, password string, rate float64) (*Chunker, error) {
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
	chunker.rate = rate

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

	var ticker *time.Ticker
	firstFrame := true
	if chunker.rate > 0 {
		interval := float64(time.Second) / chunker.rate
		ticker = time.NewTicker(time.Duration(interval))
	}

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

		select { // check for stop
		case <-chunker.stop:
			break ChunkLoop
		default:
		}

		if !firstFrame && ticker != nil {
			select {
			case <-ticker.C: // use frame
			default: // skip frame
				continue ChunkLoop
			}
		}

		firstFrame = false
		pubChan <- data
	}

	if ticker != nil {
		ticker.Stop()
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
