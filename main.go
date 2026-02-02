package main

import (
	"bufio"
	"fmt"
	"net"
	"strings"
)

func main() {
	listener, err := net.Listen("tcp", ":8080")
	if err != nil {
		panic(err)
	}
	defer listener.Close()

	fmt.Println("HTTP Server listening on :8080")

	for {
		conn, err := listener.Accept()
		if err != nil {
			continue
		}
		go handleHTTPConnection(conn)
	}
}

func handleHTTPConnection(conn net.Conn) {
	defer conn.Close()

	// bufio.Reader wraps the connection for efficient reading
	reader := bufio.NewReader(conn)

	// Read the request line (e.g., "GET /index.html HTTP/1.1")
	requestLine, err := reader.ReadString('\n')
	if err != nil {
		return
	}

	// Parse request line
	parts := strings.Fields(requestLine) // Split by whitespace
	if len(parts) < 3 {
		sendResponse(conn, 400, "Bad Request")
		return
	}

	method := parts[0] // "GET"
	path := parts[1]   // "/index.html"

	fmt.Printf("%s %s\n", method, path)

	// Read headers (until blank line)
	for {
		line, err := reader.ReadString('\n')
		if err != nil || line == "\r\n" {
			break // End of headers
		}
	}

	// Route the request
	if method == "GET" && path == "/" {
		sendResponse(conn, 200, "Welcome to our web server!")
	} else if method == "GET" && path == "/hello" {
		sendResponse(conn, 200, "Hello, World!")
	} else {
		sendResponse(conn, 404, "Not Found")
	}
}

func sendResponse(conn net.Conn, status int, body string) {
	statusText := map[int]string{
		200: "OK",
		404: "Not Found",
		400: "Bad Request",
	}

	response := fmt.Sprintf(
		"HTTP/1.1 %d %s\r\n"+
			"Content-Type: text/plain\r\n"+
			"Content-Length: %d\r\n"+
			"\r\n"+
			"%s",
		status, statusText[status], len(body), body,
	)

	conn.Write([]byte(response))
}
