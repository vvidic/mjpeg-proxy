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
	"crypto/md5"
	"crypto/rand"
	"fmt"
	"io"
	"net/http"
	"strings"
)

func digestAuthRequested(resp *http.Response) bool {
	return resp.StatusCode == http.StatusUnauthorized &&
		strings.HasPrefix(resp.Header.Get("WWW-Authenticate"), "Digest ")
}

func digestAuthBuild(username, password, uri string, resp *http.Response) string {
	auth := strings.TrimPrefix(resp.Header.Get("WWW-Authenticate"), "Digest ")
	authMap := make(map[string]string)
	authQop := false
	for _, part := range strings.Split(auth, ",") {
		kv := strings.SplitN(strings.TrimSpace(part), "=", 2)
		key := kv[0]
		val := strings.Trim(kv[1], `"`)
		authMap[key] = val
		if key == "qop" {
			for _, qop := range strings.Split(val, ",") {
				if qop == "auth" {
					authQop = true
					break
				}
			}
		}
	}

	a1 := fmt.Sprintf("%s:%s:%s", username, authMap["realm"], password)
	h1 := md5.New()
	io.WriteString(h1, a1)
	h1hex := fmt.Sprintf("%x", h1.Sum(nil))

	a2 := fmt.Sprintf("%s:%s", "GET", uri)
	h2 := md5.New()
	io.WriteString(h2, a2)
	h2hex := fmt.Sprintf("%x", h2.Sum(nil))

	b := make([]byte, 8)
	rand.Read(b)
	cnonce := fmt.Sprintf("%x", b)
	nc := fmt.Sprintf("%08x", 1)

	a := fmt.Sprintf("%s:%s:", h1hex, authMap["nonce"])
	if authQop {
		a += fmt.Sprintf("%s:%s:%s:", nc, cnonce, "auth")
	}
	a += h2hex
	h := md5.New()
	io.WriteString(h, a)
	response := fmt.Sprintf("%x", h.Sum(nil))

	result := fmt.Sprintf(`username="%s", realm="%s", nonce="%s", response="%s", uri="%s"`,
		username, authMap["realm"], authMap["nonce"], response, uri)

	if authQop {
		result += fmt.Sprintf(`, nc=%s, cnonce="%s", qop=%s, algorithm=%s`,
			nc, cnonce, "auth", "MD5")
	}

	if opaque, found := authMap["opaque"]; found {
		result += fmt.Sprintf(`, opaque="%s"`, opaque)
	}

	return result
}
