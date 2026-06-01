/*
 * MinIO Cloud Storage, (C) 2016-2020 MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cmd

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gofiber/fiber/v3"
)

// TestStreamHandlerStreamsBodyWithContentLength verifies that the streaming
// bridge applies the handler status/headers (preserving literal casing such as
// "ETag"), honours a handler-declared Content-Length, and streams the body
// written across multiple Write calls.
func TestStreamHandlerStreamsBodyWithContentLength(t *testing.T) {
	app := newFiberApp()
	app.Get("/stream", toMinioStreamHandler(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header()["ETag"] = []string{`"abc"`}
		w.Header().Set("Content-Length", "6")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("foo"))
		_, _ = w.Write([]byte("bar"))
	}))

	handler := fiberHTTPTestHandler(app)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/stream", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if rec.Body.String() != "foobar" {
		t.Fatalf("expected streamed body foobar, got %q", rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/octet-stream" {
		t.Fatalf("expected octet-stream content-type, got %q", ct)
	}
	if cl := rec.Header().Get("Content-Length"); cl != "6" {
		t.Fatalf("expected Content-Length 6, got %q", cl)
	}
	foundETag := false
	for k := range rec.Header() {
		if k == "ETag" {
			foundETag = true
			break
		}
	}
	if !foundETag {
		t.Fatalf("expected literal ETag header casing preserved, headers=%v", rec.Header())
	}
}

// TestStreamHandlerCompletionDeferred verifies that a completion hook registered
// by an outer wrapper (mirroring maxClients / stats) does NOT fire when the
// streaming handler returns, but fires exactly once after the body is consumed.
func TestStreamHandlerCompletionDeferred(t *testing.T) {
	app := newFiberApp()

	var ranAtReturn bool
	var completions int

	wrapped := func(c fiber.Ctx) error {
		err := toMinioStreamHandler(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Length", "6")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("foobar"))
		})(c)
		// The streamed body has not been written yet at this point, so the
		// completion barrier must exist and the hook must not have run.
		if sc := streamCompletionOf(c); sc != nil {
			sc.add(func() { completions++ })
		}
		return err
	}
	app.Get("/stream", func(c fiber.Ctx) error {
		e := wrapped(c)
		ranAtReturn = completions > 0
		return e
	})

	handler := fiberHTTPTestHandler(app)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/stream", nil))

	if rec.Body.String() != "foobar" {
		t.Fatalf("expected streamed body foobar, got %q", rec.Body.String())
	}
	if ranAtReturn {
		t.Fatalf("completion hook ran before the body was streamed")
	}
	if completions != 1 {
		t.Fatalf("expected completion hook to run exactly once, got %d", completions)
	}
}

// TestStreamHandlerErrorResponse verifies that a handler which writes an error
// status before any body still streams correctly with the right status code.
func TestStreamHandlerErrorResponse(t *testing.T) {
	app := newFiberApp()
	app.Get("/stream", toMinioStreamHandler(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte("<Error/>"))
	}))

	handler := fiberHTTPTestHandler(app)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/stream", nil))

	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rec.Code)
	}
	if rec.Body.String() != "<Error/>" {
		t.Fatalf("expected error body, got %q", rec.Body.String())
	}
}
