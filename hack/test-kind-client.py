#!/usr/bin/env python3
"""Exercise the client pod's embedded programs without requiring Kubernetes."""
import io
import pathlib
import re
import subprocess
import sys
import unittest
from unittest.mock import patch
from urllib.error import HTTPError

HELPER = pathlib.Path(__file__).with_name("kind-client.sh")
PROGRAMS = re.findall(r"<<'PY'\n(.*?)\nPY", HELPER.read_text(), re.S)


class Output:
    def __init__(self):
        self.buffer = io.BytesIO()

    def write(self, text):
        return self.buffer.write(text.encode())


class Connection:
    def __init__(self, reply):
        self.reply = reply
        self.request = None

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def sendall(self, request):
        self.request = request

    def makefile(self, mode):
        return io.BytesIO(self.reply)


class ClientTests(unittest.TestCase):
    def run_program(self, index, args):
        output = Output()
        with patch.object(sys, "argv", ["-", *args]), patch.object(sys, "stdout", output):
            exec(compile(PROGRAMS[index], str(HELPER), "exec"), {})
        return output.buffer.getvalue()

    def test_binary_nested_redis_reply_and_utf8_request(self):
        reply = b'*3\r\n$3\r\na\x00b\r\n$-1\r\n*2\r\n:1\r\n+OK\r\n'
        conn = Connection(reply)
        with patch("socket.create_connection", return_value=conn) as connect:
            actual = self.run_program(0, ["demo-0.demo-headless", "GET", "café"])
        self.assertEqual(actual, reply)
        self.assertEqual(conn.request, b'*2\r\n$3\r\nGET\r\n$5\r\ncaf\xc3\xa9\r\n')
        connect.assert_called_once_with(("demo-0.demo-headless", 6379), timeout=5)

    def test_incomplete_redis_reply_fails(self):
        for reply in [b'', b'$4\r\nab', b'*2\r\n+OK\r\n', b'garbage\r\n']:
            with self.subTest(reply=reply), patch("socket.create_connection", return_value=Connection(reply)):
                with self.assertRaises(RuntimeError):
                    self.run_program(0, ["demo-0.demo-headless", "GET", "key"])

    def test_http_status_is_not_inferred_from_tool_failure(self):
        for status in [401, 403, 500]:
            error = HTTPError("http://test", status, "error", {}, io.BytesIO(b'error'))
            with patch("urllib.request.urlopen", side_effect=error):
                self.assertEqual(self.run_program(1, ["http://test", "status", "POST"]), f"{status}\n".encode())
        with patch("urllib.request.urlopen", side_effect=OSError("connection refused")):
            with self.assertRaises(OSError):
                self.run_program(1, ["http://test", "status", "POST"])

    def test_http_body_requires_success(self):
        error = HTTPError("http://test", 500, "error", {}, io.BytesIO(b'error'))
        with patch("urllib.request.urlopen", side_effect=error), self.assertRaises(RuntimeError):
            self.run_program(1, ["http://test", "body", "GET"])

    def test_authentication_assertion_requires_exact_401(self):
        for response, exit_code, success in [("401", 0, True), ("200", 0, False), ("500", 0, False), ("", 127, False), ("401", 1, False)]:
            with self.subTest(response=response, exit_code=exit_code):
                result = subprocess.run([
                    "bash", "-c",
                    'source "$1"; http_on_pod() { printf "%s" "$RESPONSE"; return "$RESULT"; }; assert_cluster_authentication',
                    "bash", str(HELPER),
                ], env={"RESPONSE": response, "RESULT": str(exit_code)}, capture_output=True)
                self.assertEqual(result.returncode == 0, success, result.stderr)


if __name__ == "__main__":
    unittest.main()
