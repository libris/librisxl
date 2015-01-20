import SimpleHTTPServer
import SocketServer

import sys

import cgi


if len(sys.argv) > 2:
    PORT = int(sys.argv[2])
    I = sys.argv[1]
elif len(sys.argv) > 1:
    PORT = int(sys.argv[1])
    I = ""
else:
    PORT = 8100
    I = ""


class ServerHandler(SimpleHTTPServer.SimpleHTTPRequestHandler):

    def do_GET(self):
        print "GET request at %s" % self.path
        self.send_response(200)
        self.send_header("Content-type", "text/plain")
        self.send_header("Content-Length", "0")
        self.end_headers()
        self.wfile.write("")
        self.wfile.close()

    def do_DELETE(self):
        print "Deleting record %s" % self.path
        self.send_response(201)
        self.end_headers()
        self.wfile.close()

    def do_PUT(self):
        #print "Received POST request."
        #print "headers", self.headers
        if self.headers['Content-Type'] != "application/xml":
            self.send_error(401, "Need XML, yo")
        else:
            content_length = int(self.headers['Content-Length'])
            put_data = self.rfile.read(content_length)
            print put_data

            if self.path == "/apix/0.1/cat/test/bib/new":
                print "Creating a new record."
                self.send_response(201)
            else:
                print "Updating record %s" % self.path
                self.send_response(303)
                self.send_header('Location', 'http://%(interface)s:%(port)s%(path)s' % dict(interface=I or "127.0.0.1", port=PORT, path=self.path))

            self.end_headers()
            self.wfile.close()

Handler = ServerHandler

httpd = SocketServer.TCPServer(("", PORT), Handler)

print "Serving at: http://%(interface)s:%(port)s" % dict(interface=I or "127.0.0.1", port=PORT)
httpd.serve_forever()
