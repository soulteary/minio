#!/usr/bin/env python3
"""Convert MinIO Handler functions from net/http to Fiber v3 using AST."""

import ast
import sys
from pathlib import Path

CMD = Path(__file__).resolve().parent.parent / "cmd"

SKIP = {
    "generic-handlers.go",
    "auth-handler.go",
    "api-response.go",
    "handler-utils.go",
    "http-tracer.go",
    "test-utils_test.go",
    "fiber_ctx.go",
    "fiber_response.go",
    "fiber_trace.go",
    "fiber_router.go",
    "fiber_api_router.go",
    "fiber_admin_router.go",
    "fiber_health_router.go",
    "fiber_metrics_router.go",
    "fiber_sts_router.go",
    "fiber_web_router.go",
    "fiber_dist_router.go",
    "fiber_rest_routers.go",
    "admin-router.go",
    "api-router.go",
    "healthcheck-router.go",
    "metrics-router.go",
    "web-router.go",
    "storage-rest-server.go",
    "peer-rest-server.go",
    "lock-rest-server.go",
    "bootstrap-peer-server.go",
    "sts-handlers.go",
    "crossdomain-xml-handler.go",
    "api-headers.go",
    "handler-api.go",
    "metrics.go",
    "sts-errors.go",
    "object-handlers-common.go",
    "utils.go",
    "gateway-main.go",
    "server-main.go",
    "http-stats.go",
    "routers.go",
}

REPLACEMENTS = [
    ("mux.Vars(r)[\"bucket\"]", "pathParamBucket(c)"),
    ("mux.Vars(r)[\"object\"]", "pathParamObject(c)"),
    ("mux.Vars(r)[\"prefix\"]", "pathParamPrefix(c)"),
    ('vars["bucket"]', "pathParamBucket(c)"),
    ('vars["object"]', "pathParamObject(c)"),
    ('vars["prefix"]', "pathParamPrefix(c)"),
    ("guessIsBrowserReq(r)", "guessIsBrowserReqFiber(c)"),
    ("writeErrorResponse(r.Context(), w,", "writeErrorResponseFiber(c.Context(), c,"),
    ("writeErrorResponseJSON(r.Context(), w,", "writeErrorResponseJSONFiber(c.Context(), c,"),
    ("writeErrorResponseString(r.Context(), w,", "writeErrorResponseStringFiber(c.Context(), c,"),
    ("writeErrorResponseHeadersOnly(w,", "writeErrorResponseHeadersOnlyFiber(c,"),
    ("writeSuccessResponseJSON(w,", "writeSuccessResponseJSONFiber(c,"),
    ("writeSuccessResponseXML(w,", "writeSuccessResponseXMLFiber(c,"),
    ("writeSuccessNoContent(w)", "writeSuccessNoContentFiber(c)"),
    ("writeSuccessResponseHeadersOnly(w)", "writeSuccessResponseHeadersOnlyFiber(c)"),
    ("writeRedirectSeeOther(w,", "writeRedirectSeeOtherFiber(c,"),
    ("writeResponse(w,", "writeResponseFiber(c,"),
    ("newContext(r, w,", "newContextFiber(c,"),
    ("r.Context()", "c.Context()"),
    ("r.Method", "c.Method()"),
    ("r.URL.Path", "c.Path()"),
    ("r.URL.RawQuery", 'string(c.Request().URI().QueryString())'),
    ("r.Header.Get(", "c.Get("),
    ("r.UserAgent()", 'c.Get("User-Agent")'),
    ("handlers.GetSourceIP(r)", "handlers.GetSourceIPFiber(c)"),
    ("handlers.GetSourceScheme(r)", "handlers.GetSourceSchemeFiber(c)"),
    ("w.Header().Set(", "c.Set("),
    ("w.Header().Get(", "c.Get("),
    ("w.WriteHeader(", "c.Status("),
    ("r.Host", "requestHost(c)"),
    (", r.URL,", ", requestURL(c),"),
    (", r.URL)", ", requestURL(c))"),
    ("httpTraceAll(", "httpTraceAllFiber(toMinioHandler("),
    ("httpTraceHdrs(", "httpTraceHdrsFiber(toMinioHandler("),
    ("collectAPIStats(", "collectAPIStatsFiber("),
    ("maxClients(", "maxClientsFiber("),
]

IMPORTS_TO_REMOVE = ["github.com/gorilla/mux", "github.com/gorilla/handlers", "github.com/rs/cors"]
FIBER_IMPORT = '"github.com/gofiber/fiber/v3"'


class HandlerConverter(ast.NodeVisitor):
    def __init__(self):
        self.source = ""
        self.lines = []

    def convert_file(self, path: Path) -> bool:
        self.source = path.read_text()
        self.lines = self.source.splitlines(keepends=True)
        try:
            tree = ast.parse(self.source)
        except SyntaxError:
            return False

        changed = False
        for node in tree.body:
            if isinstance(node, ast.FunctionDef):
                if self._is_handler_func(node):
                    if self._convert_handler(node):
                        changed = True
        if not changed:
            return False

        result = "".join(self.lines)
        if FIBER_IMPORT not in result:
            result = result.replace("import (\n", f"import (\n\t{FIBER_IMPORT}\n", 1)
        for imp in IMPORTS_TO_REMOVE:
            result = result.replace(f'\t"{imp}"\n', "")
        path.write_text(result)
        return True

    def _is_handler_func(self, node: ast.FunctionDef) -> bool:
        if len(node.args.args) != 2:
            return False
        a0, a1 = node.args.args[0].arg, node.args.args[1].arg
        if a0 != "w" or a1 != "r":
            return False
        if node.returns is not None:
            return False
        return "Handler" in node.name or node.name.endswith("Handler")

    def _convert_handler(self, node: ast.FunctionDef) -> bool:
        start = node.lineno - 1
        end = node.end_lineno
        block = "".join(self.lines[start:end])

        # signature
        recv = ""
        if node.name and hasattr(node, "name"):
            pass
        # rebuild signature from original line
        first_line = self.lines[start]
        if "func (" in first_line:
            sig_end = first_line.index(")") + 1
            receiver = first_line[5:first_line.index(")")]
            name = node.name
            new_sig = f"func ({receiver}) {name}(c fiber.Ctx) error {{\n"
        else:
            new_sig = f"func {node.name}(c fiber.Ctx) error {{\n"

        body_lines = self.lines[start + 1 : end - 1]
        body = "".join(body_lines)

        for old, new in REPLACEMENTS:
            body = body.replace(old, new)

        # fix writeErrorResponseFiber extra url arg
        body = body.replace(", requestURL(c), guessIsBrowserReqFiber(c)", ", guessIsBrowserReqFiber(c)")

        # ensure returns
        if "return nil" not in body and "return " not in body.strip().split("\n")[-1]:
            body = body.rstrip() + "\n\treturn nil\n"

        new_block = new_sig + body + "}\n"
        self.lines[start:end] = [new_block]
        return True


def main():
    conv = HandlerConverter()
    for path in sorted(CMD.glob("*.go")):
        if path.name in SKIP:
            continue
        if conv.convert_file(path):
            print(f"converted: {path.name}")


if __name__ == "__main__":
    main()
