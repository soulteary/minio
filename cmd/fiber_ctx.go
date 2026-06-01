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
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"

	"github.com/gofiber/fiber/v3"
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
		// strings.Clone detaches the value from the fasthttp request buffer:
		// fiber recycles that buffer once the request completes, so any view
		// handed to a goroutine that outlives the request (e.g. FS
		// backgroundAppend, metacache writers) would otherwise race with the
		// next request's ctx.Reset and read corrupted bytes.
		if v := c.Params(name); v != "" {
			m[name] = strings.Clone(v)
		}
	}
	if routeHasPathWildcard(c) {
		if wild := strings.TrimPrefix(c.Params("*"), "/"); wild != "" {
			if _, ok := m["object"]; !ok {
				m["object"] = strings.Clone(likelyUnescapeGeneric(wild, url.PathUnescape))
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

// fiberStreamResponseWriter bridges a legacy net/http handler to a streamed
// Fiber response. The handler runs in its own goroutine and writes flow through
// an io.Pipe; the body is sent to the client via fasthttp's SetBodyStream so the
// full payload is never buffered in memory (important for large GetObject reads).
type fiberStreamResponseWriter struct {
	header      http.Header
	status      int
	pw          *io.PipeWriter
	ready       chan struct{}
	once        sync.Once
	wroteHeader bool
	panicVal    interface{}
}

func (w *fiberStreamResponseWriter) Header() http.Header { return w.header }

func (w *fiberStreamResponseWriter) WriteHeader(statusCode int) {
	if w.wroteHeader {
		return
	}
	w.wroteHeader = true
	w.status = statusCode
	w.signalReady()
}

func (w *fiberStreamResponseWriter) Write(b []byte) (int, error) {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	return w.pw.Write(b)
}

func (w *fiberStreamResponseWriter) Flush() {}

// signalReady unblocks the dispatcher once status and headers are final.
func (w *fiberStreamResponseWriter) signalReady() {
	w.once.Do(func() { close(w.ready) })
}

// toMinioStreamHandler adapts a legacy net/http handler to a streaming
// MinioHandler. Headers/status are captured from the first write and applied to
// the response, then the body is streamed from the handler goroutine.
func toMinioStreamHandler(h func(http.ResponseWriter, *http.Request)) MinioHandler {
	return func(c fiber.Ctx) error {
		r, err := fiberRequest(c)
		if err != nil {
			return err
		}
		r = setURLVarsOnRequest(r, allPathParams(c))

		pr, pw := io.Pipe()
		w := &fiberStreamResponseWriter{
			header: seedResponseHeader(c),
			status: http.StatusOK,
			pw:     pw,
			ready:  make(chan struct{}),
		}

		go func() {
			defer func() {
				if rec := recover(); rec != nil {
					// Do NOT re-panic here: this is a child goroutine and a panic
					// would bypass criticalErrorHandlerFiber and crash the process.
					// Record it and let the dispatcher re-raise it on the request
					// goroutine (when no response has been committed yet).
					w.panicVal = rec
					_ = pw.CloseWithError(io.ErrClosedPipe)
					w.signalReady()
					return
				}
				w.signalReady()
				_ = pw.Close()
			}()
			h(w, r)
		}()

		<-w.ready

		// If the handler panicked before producing any output, re-raise on this
		// (request) goroutine so criticalErrorHandlerFiber / the server recover
		// can turn it into a proper error response, matching the buffered path.
		if w.panicVal != nil && !w.wroteHeader {
			panic(w.panicVal)
		}

		// Apply captured headers (preserving casing) before sending the body.
		c.Response().Header.DisableNormalizing()
		contentLength := int64(-1)
		for k, vv := range w.header {
			if http.CanonicalHeaderKey(k) == "Content-Length" {
				if len(vv) > 0 {
					if n, perr := strconv.ParseInt(vv[0], 10, 64); perr == nil {
						contentLength = n
					}
				}
				continue
			}
			c.Response().Header.Del(k)
			for _, v := range vv {
				c.Response().Header.Add(k, v)
			}
		}
		c.Status(w.status)

		// Register a stream-completion barrier so wrappers (maxClients, stats)
		// can hold their slot / defer measurement until the body has been fully
		// written by fasthttp, which happens AFTER this handler returns. This
		// restores the net/http semantics where the handler streamed the body
		// inline and only returned once the transfer completed.
		sc := &streamCompletion{}
		c.RequestCtx().SetUserValue(streamCompletionKey{}, sc)
		body := streamCompletionReader{r: pr, sc: sc}
		if contentLength >= 0 {
			c.Response().SetBodyStream(body, int(contentLength))
		} else {
			c.Response().SetBodyStream(body, -1)
		}
		return nil
	}
}

// streamCompletion collects callbacks to run once a streamed response body has
// been fully written (or aborted). The body stream is consumed by fasthttp on
// the same connection goroutine AFTER the handler chain returns, so this lets
// resource/measurement wrappers span the streaming phase.
type streamCompletion struct {
	mu    sync.Mutex
	ran   bool
	hooks []func()
}

func (s *streamCompletion) add(fn func()) {
	s.mu.Lock()
	if s.ran {
		s.mu.Unlock()
		fn()
		return
	}
	s.hooks = append(s.hooks, fn)
	s.mu.Unlock()
}

func (s *streamCompletion) run() {
	s.mu.Lock()
	if s.ran {
		s.mu.Unlock()
		return
	}
	s.ran = true
	hooks := s.hooks
	s.hooks = nil
	s.mu.Unlock()
	for _, fn := range hooks {
		fn()
	}
}

type streamCompletionKey struct{}

// streamCompletionOf returns the per-request stream-completion barrier if the
// handler set up a streamed response, or nil otherwise.
func streamCompletionOf(c fiber.Ctx) *streamCompletion {
	if v := c.RequestCtx().UserValue(streamCompletionKey{}); v != nil {
		if sc, ok := v.(*streamCompletion); ok {
			return sc
		}
	}
	return nil
}

// streamCompletionReader wraps the pipe reader handed to fasthttp so that the
// completion hooks fire when fasthttp closes the stream (end of body or abort).
type streamCompletionReader struct {
	r  *io.PipeReader
	sc *streamCompletion
}

func (r streamCompletionReader) Read(p []byte) (int, error) { return r.r.Read(p) }

func (r streamCompletionReader) Close() error {
	err := r.r.Close()
	r.sc.run()
	return err
}

const fiberObjectParam = "object"
const fiberBucketParam = "bucket"
const fiberPrefixParam = "prefix"

// fiberVhostBucketParam is the Locals key under which vhostBucketMiddleware
// stores a bucket extracted from the Host header (virtual-host-style request).
// It is distinct from fiberBucketParam so the API dispatcher can tell that the
// entire request path is the object key rather than "/bucket/object".
const fiberVhostBucketParam = "vhostBucket"

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
// All path-param accessors return strings.Clone'd values: fiber/fasthttp back
// path params with the recycled request buffer, so any value that may be held
// past the request (object names handed to FS backgroundAppend, metacache
// writers, request contexts, etc.) must be detached to avoid a use-after-reset
// data race.
func pathParamObject(c fiber.Ctx) string {
	if object, ok := c.Locals(fiberObjectParam).(string); ok && object != "" {
		return strings.Clone(likelyUnescapeGeneric(object, url.PathUnescape))
	}
	obj := c.Params(fiberObjectParam)
	if obj == "" && routeHasPathWildcard(c) {
		obj = strings.TrimPrefix(c.Params("*"), "/")
	}
	return strings.Clone(likelyUnescapeGeneric(obj, url.PathUnescape))
}

// pathParamBucket returns the bucket name from Fiber path params or vhost locals.
func pathParamBucket(c fiber.Ctx) string {
	if bucket, ok := c.Locals(fiberVhostBucketParam).(string); ok && bucket != "" {
		return strings.Clone(bucket)
	}
	if bucket, ok := c.Locals(fiberBucketParam).(string); ok && bucket != "" {
		return strings.Clone(bucket)
	}
	return strings.Clone(c.Params(fiberBucketParam))
}

// pathParamPrefix returns the prefix param used by admin heal routes.
func pathParamPrefix(c fiber.Ctx) string {
	return strings.Clone(likelyUnescapeGeneric(c.Params(fiberPrefixParam), url.QueryUnescape))
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
		RequestID:    fiberRequestID(c),
		RemoteHost:   handlers.GetSourceIPFiber(c),
		Host:         getHostNameFiber(c),
		UserAgent:    c.Get("User-Agent"),
		API:          api,
		BucketName:   bucket,
		ObjectName:   object,
	}
	return logger.SetReqInfo(c.Context(), reqInfo)
}

// fiberRequestID returns the x-amz-request-id value. The addCustomHeaders
// middleware sets it on the response header (not the request header), so it
// must be read back from the response rather than via c.Get (request header).
func fiberRequestID(c fiber.Ctx) string {
	return string(c.Response().Header.Peek(xhttp.AmzRequestID))
}

func getHostNameFiber(c fiber.Ctx) (hostName string) {
	if globalIsDistErasure {
		hostName = globalLocalNodeName
	} else {
		hostName = requestHost(c)
	}
	return
}

// fiberRequestBody returns the request body as an io.ReadCloser. When fasthttp
// exposes a body stream (StreamRequestBody and the body not yet materialized)
// it is wired directly so the handler reads incrementally; otherwise it falls
// back to the already-buffered body. This avoids the full in-memory copy that
// adaptor.ConvertRequest performs via PostBody().
//
// Crucially it reads the RAW fasthttp body (c.Request().Body()) rather than
// fiber's c.Body(): the latter transparently decodes per Content-Encoding,
// which corrupts S3 payloads that legitimately carry an encoding header such as
// "aws-chunked" streaming-signature uploads (the handler must see the original
// chunked bytes to verify chunk signatures).
func fiberRequestBody(c fiber.Ctx) io.ReadCloser {
	if bs := c.Request().BodyStream(); bs != nil {
		return io.NopCloser(bs)
	}
	return io.NopCloser(bytes.NewReader(c.Request().Body()))
}

// fiberRequest converts a Fiber context to *http.Request for auth/policy
// compatibility. Unlike adaptor.ConvertRequest it does not call PostBody(), so
// the request body is not forcibly buffered into memory here.
func fiberRequest(c fiber.Ctx) (*http.Request, error) {
	fctx := c.RequestCtx()
	reqURI := string(fctx.RequestURI())
	u, err := url.ParseRequestURI(reqURI)
	if err != nil {
		return nil, err
	}

	r := &http.Request{
		Method:     string(fctx.Method()),
		Proto:      string(fctx.Request.Header.Protocol()),
		ProtoMajor: 1,
		ProtoMinor: 1,
		URL:        u,
		RequestURI: reqURI,
		RemoteAddr: fctx.RemoteAddr().String(),
		Host:       string(fctx.Host()),
		TLS:        fctx.TLSConnectionState(),
		Header:     make(http.Header),
	}
	if r.Proto == "HTTP/2" {
		r.ProtoMajor = 2
	}

	// VisitAll yields each stored header occurrence exactly once, so Add (rather
	// than Set) faithfully reproduces net/http multi-value header semantics that
	// legacy handlers expect, instead of collapsing duplicates to the last value.
	fctx.Request.Header.VisitAll(func(k, v []byte) {
		sk := string(k)
		sv := string(v)
		if sk == "Transfer-Encoding" {
			r.TransferEncoding = append(r.TransferEncoding, sv)
			return
		}
		r.Header.Add(sk, sv)
	})

	r.Body = fiberRequestBody(c)
	r.ContentLength = int64(fctx.Request.Header.ContentLength())

	// Request-scoped Content-Length override used by the httptest bridge
	// (fiberHTTPTestHandler). Stored on the per-request fasthttp ctx instead of
	// package globals so concurrent tests do not race.
	if v := fctx.UserValue(testContentLengthKey{}); v != nil {
		if n, ok := v.(int64); ok {
			r.ContentLength = n
		}
	}
	return r, nil
}

// guessIsBrowserReqFiber checks if the request is from a browser.
func guessIsBrowserReqFiber(c fiber.Ctx) bool {
	// Cheap precondition straight off the fasthttp request; only build an
	// *http.Request for the auth-type classification when it can still match.
	if !globalBrowserEnabled || !strings.Contains(c.Get("User-Agent"), "Mozilla") {
		return false
	}
	r, err := fiberRequest(c)
	if err != nil {
		return false
	}
	return guessIsBrowserReq(r)
}

func guessIsHealthCheckReqFiber(c fiber.Ctx) bool {
	if method := c.Method(); method != fiber.MethodGet && method != fiber.MethodHead {
		return false
	}
	switch c.Path() {
	case healthCheckPathPrefix + healthCheckLivenessPath,
		healthCheckPathPrefix + healthCheckReadinessPath,
		healthCheckPathPrefix + healthCheckClusterPath,
		healthCheckPathPrefix + healthCheckClusterReadPath:
	default:
		return false
	}
	r, err := fiberRequest(c)
	if err != nil {
		return false
	}
	return guessIsHealthCheckReq(r)
}

func guessIsMetricsReqFiber(c fiber.Ctx) bool {
	switch c.Path() {
	case minioReservedBucketPath + prometheusMetricsPathLegacy,
		minioReservedBucketPath + prometheusMetricsV2ClusterPath,
		minioReservedBucketPath + prometheusMetricsV2NodePath:
	default:
		return false
	}
	r, err := fiberRequest(c)
	if err != nil {
		return false
	}
	return guessIsMetricsReq(r)
}

// guessIsRPCReqFiber mirrors guessIsRPCReq but reads the method/path directly
// from fasthttp, avoiding an *http.Request allocation entirely.
func guessIsRPCReqFiber(c fiber.Ctx) bool {
	return c.Method() == fiber.MethodPost &&
		strings.HasPrefix(c.Path(), minioReservedBucketPath+SlashSeparator)
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

// seedResponseHeader returns a fresh http.Header pre-populated with the
// x-amz-request-id that the addCustomHeaders middleware set on the response
// header. Legacy net/http handlers (and api-response.go helpers) read it back
// via w.Header().Get(xhttp.AmzRequestID); without seeding they would observe an
// empty value because the bridge writer starts with a fresh map.
func seedResponseHeader(c fiber.Ctx) http.Header {
	header := make(http.Header)
	if reqID := c.Response().Header.Peek(xhttp.AmzRequestID); len(reqID) > 0 {
		header.Set(xhttp.AmzRequestID, string(reqID))
	}
	return header
}

func newFiberResponseWriter(c fiber.Ctx) *fiberResponseWriter {
	return &fiberResponseWriter{
		c:      c,
		header: seedResponseHeader(c),
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
