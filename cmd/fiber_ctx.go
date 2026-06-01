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
	"bufio"
	"context"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"

	"github.com/gofiber/fiber/v3"
	"github.com/gofiber/fiber/v3/middleware/adaptor"
	xhttp "github.com/minio/minio/cmd/http"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/handlers"
)

// MinioHandler is the standard Fiber handler signature for MinIO APIs.
type MinioHandler = fiber.Handler

// urlVarsKey is the context key for path variables bridged from Fiber.
type urlVarsKey struct{}

func setURLVarsOnRequest(r *http.Request, vars map[string]string) *http.Request {
	ctx := context.WithValue(r.Context(), urlVarsKey{}, vars)
	return r.WithContext(ctx)
}

func urlVar(r *http.Request, key string) string {
	if vars, ok := r.Context().Value(urlVarsKey{}).(map[string]string); ok {
		return vars[key]
	}
	return ""
}

func urlVars(r *http.Request) map[string]string {
	if vars, ok := r.Context().Value(urlVarsKey{}).(map[string]string); ok {
		return vars
	}
	return map[string]string{}
}

func routeHasPathWildcard(c fiber.Ctx) bool {
	if route := c.Route(); route != nil {
		return strings.Contains(route.Path, "*")
	}
	return false
}

func allPathParams(c fiber.Ctx) map[string]string {
	m := make(map[string]string)
	if b := pathParamBucket(c); b != "" {
		m["bucket"] = b
	}
	if o := pathParamObject(c); o != "" {
		m["object"] = o
	}
	if p := pathParamPrefix(c); p != "" {
		m["prefix"] = p
	}
	_ = c.Route()
	for _, name := range []string{
		"accessKey", "name", "group", "policy", "action", "updateURL", "profiler",
		"api", "uploadId", "partNumber", "token", "events", "user", "serviceAccount",
		"bucket", "object", "prefix", "file", "ext", "index", "assets",
	} {
		if v := c.Params(name); v != "" {
			m[name] = v
		}
	}
	if routeHasPathWildcard(c) {
		if wild := strings.TrimPrefix(c.Params("*"), "/"); wild != "" {
			if _, ok := m["object"]; !ok {
				m["object"] = likelyUnescapeGeneric(wild, url.PathUnescape)
			}
		}
	}
	return m
}

// toMinioHandler adapts a legacy net/http handler to MinioHandler.
func toMinioHandler(h func(http.ResponseWriter, *http.Request)) MinioHandler {
	return func(c fiber.Ctx) error {
		r, err := fiberRequest(c)
		if err != nil {
			return err
		}
		r = setURLVarsOnRequest(r, allPathParams(c))
		w := newFiberResponseWriter(c)
		h(w, r)
		w.finalize()
		return nil
	}
}

// minioHandlerToHTTP adapts a MinioHandler to net/http for legacy routers and tests.
func minioHandlerToHTTP(h MinioHandler) http.HandlerFunc {
	return adaptor.FiberHandler(h).ServeHTTP
}

const fiberObjectParam = "object"
const fiberBucketParam = "bucket"
const fiberPrefixParam = "prefix"

// requestURL returns a *url.URL for the current Fiber request.
func requestURL(c fiber.Ctx) *url.URL {
	u, err := url.ParseRequestURI(c.OriginalURL())
	if err != nil {
		return &url.URL{
			Path:     c.Path(),
			RawQuery: string(c.Request().URI().QueryString()),
		}
	}
	return u
}

// requestHost returns the Host header value for the current request.
func requestHost(c fiber.Ctx) string {
	host := c.Hostname()
	if port := c.Port(); port != "" && !strings.Contains(host, ":") {
		return net.JoinHostPort(host, port)
	}
	return host
}

// pathParamObject returns the object key from Fiber path params.
func pathParamObject(c fiber.Ctx) string {
	if object, ok := c.Locals(fiberObjectParam).(string); ok && object != "" {
		return likelyUnescapeGeneric(object, url.PathUnescape)
	}
	obj := c.Params(fiberObjectParam)
	if obj == "" && routeHasPathWildcard(c) {
		obj = strings.TrimPrefix(c.Params("*"), "/")
	}
	return likelyUnescapeGeneric(obj, url.PathUnescape)
}

// pathParamBucket returns the bucket name from Fiber path params or vhost locals.
func pathParamBucket(c fiber.Ctx) string {
	if bucket, ok := c.Locals(fiberBucketParam).(string); ok && bucket != "" {
		return bucket
	}
	return c.Params(fiberBucketParam)
}

// pathParamPrefix returns the prefix param used by admin heal routes.
func pathParamPrefix(c fiber.Ctx) string {
	return likelyUnescapeGeneric(c.Params(fiberPrefixParam), url.QueryUnescape)
}

// setPathVars stores bucket/object on the context for helpers that read mux-style vars.
func setPathVars(c fiber.Ctx, bucket, object string) {
	if bucket != "" {
		c.Locals(fiberBucketParam, bucket)
	}
	if object != "" {
		c.Locals(fiberObjectParam, object)
	}
}

// newContextFiber returns context with ReqInfo details set from a Fiber request.
func newContextFiber(c fiber.Ctx, api string) context.Context {
	bucket := pathParamBucket(c)
	object := pathParamObject(c)
	prefix := pathParamPrefix(c)
	if prefix != "" {
		object = prefix
	}
	reqInfo := &logger.ReqInfo{
		DeploymentID: globalDeploymentID,
		RequestID:    c.Get(xhttp.AmzRequestID),
		RemoteHost:   handlers.GetSourceIPFiber(c),
		Host:         getHostNameFiber(c),
		UserAgent:    c.Get("User-Agent"),
		API:          api,
		BucketName:   bucket,
		ObjectName:   object,
	}
	return logger.SetReqInfo(c.Context(), reqInfo)
}

func getHostNameFiber(c fiber.Ctx) (hostName string) {
	if globalIsDistErasure {
		hostName = globalLocalNodeName
	} else {
		hostName = requestHost(c)
	}
	return
}

// fiberRequest converts a Fiber context to *http.Request for auth/policy compatibility.
func fiberRequest(c fiber.Ctx) (*http.Request, error) {
	r, err := adaptor.ConvertRequest(c, true)
	if err != nil {
		return nil, err
	}
	if testUnknownContentLength {
		r.ContentLength = -1
	} else if testDeclaredContentLength != testContentLengthUnset {
		r.ContentLength = testDeclaredContentLength
	}
	return r, nil
}

// guessIsBrowserReqFiber checks if the request is from a browser.
func guessIsBrowserReqFiber(c fiber.Ctx) bool {
	r, err := fiberRequest(c)
	if err != nil {
		return false
	}
	return guessIsBrowserReq(r)
}

func guessIsHealthCheckReqFiber(c fiber.Ctx) bool {
	r, err := fiberRequest(c)
	if err != nil {
		return false
	}
	return guessIsHealthCheckReq(r)
}

func guessIsMetricsReqFiber(c fiber.Ctx) bool {
	r, err := fiberRequest(c)
	if err != nil {
		return false
	}
	return guessIsMetricsReq(r)
}

func guessIsRPCReqFiber(c fiber.Ctx) bool {
	r, err := fiberRequest(c)
	if err != nil {
		return false
	}
	return guessIsRPCReq(r)
}

func isAdminReqFiber(c fiber.Ctx) bool {
	return strings.HasPrefix(c.Path(), adminPathPrefix)
}

// fiberResponseWriter adapts fiber.Ctx to http.ResponseWriter for legacy helpers.
type fiberResponseWriter struct {
	c           fiber.Ctx
	header      http.Header
	wroteHeader bool
	status      int
}

func newFiberResponseWriter(c fiber.Ctx) *fiberResponseWriter {
	return &fiberResponseWriter{
		c:      c,
		header: make(http.Header),
		status: http.StatusOK,
	}
}

func (w *fiberResponseWriter) Header() http.Header {
	if w.header == nil {
		w.header = make(http.Header)
	}
	return w.header
}

func (w *fiberResponseWriter) syncHeaders() {
	// Preserve the exact header-name casing chosen by legacy handlers (e.g. the
	// literal "ETag" set via direct map assignment for broken S3 clients).
	w.c.Response().Header.DisableNormalizing()
	for k, vv := range w.header {
		w.c.Response().Header.Del(k)
		for _, v := range vv {
			w.c.Response().Header.Add(k, v)
		}
	}
}

func (w *fiberResponseWriter) Write(b []byte) (int, error) {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	return w.c.Write(b)
}

func (w *fiberResponseWriter) WriteHeader(statusCode int) {
	if w.wroteHeader {
		return
	}
	w.wroteHeader = true
	w.status = statusCode
	w.syncHeaders()
	w.c.Status(statusCode)
}

func (w *fiberResponseWriter) Flush() {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	if bw := w.c.Response().BodyWriter(); bw != nil {
		if f, ok := bw.(interface{ Flush() error }); ok {
			_ = f.Flush()
		}
	}
}

// finalize applies the default status when a legacy handler did not write a response.
func (w *fiberResponseWriter) finalize() {
	if !w.wroteHeader {
		w.WriteHeader(w.status)
	}
}

// fiberBodyReader wraps the Fiber request body as io.ReadCloser.
type fiberBodyReader struct {
	c fiber.Ctx
}

func (r fiberBodyReader) Read(p []byte) (int, error) {
	return r.c.Request().BodyStream().Read(p)
}

func (r fiberBodyReader) Close() error {
	return nil
}

// fiberBodyStreamWriter provides a streaming response body writer.
type fiberBodyStreamWriter struct {
	c fiber.Ctx
}

func (w fiberBodyStreamWriter) Write(p []byte) (int, error) {
	return w.c.Write(p)
}

func (w fiberBodyStreamWriter) Flush() {
	if bw := w.c.Response().BodyWriter(); bw != nil {
		if f, ok := bw.(interface{ Flush() error }); ok {
			_ = f.Flush()
		}
	}
}

// setBodyStreamWriter sets a streaming response on the Fiber context.
func setBodyStreamWriter(c fiber.Ctx, fn func(w *bufio.Writer)) {
	c.Set("Transfer-Encoding", "chunked")
	c.Response().SetBodyStreamWriter(func(w *bufio.Writer) {
		fn(w)
	})
}

// fiberReadCloser returns the request body as io.ReadCloser.
func fiberReadCloser(c fiber.Ctx) io.ReadCloser {
	return fiberBodyReader{c: c}
}
