package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
)

const ROOT_DIR = "./public" // Serve files from this directory

func main() {
	// Create public directory if it doesn't exist
	os.MkdirAll(ROOT_DIR, 0755)

	listener, err := net.Listen("tcp", ":8080")
	if err != nil {
		panic(err)
	}
	defer listener.Close()

	fmt.Println("HTTP Server listening on :8080")
	fmt.Println("Serving files from:", ROOT_DIR)

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

	reader := bufio.NewReader(conn)

	// Parse request
	requestLine, err := reader.ReadString('\n')
	if err != nil {
		return
	}

	parts := strings.Fields(requestLine)
	if len(parts) < 3 {
		sendError(conn, 400, "Bad Request")
		return
	}

	method := parts[0]
	path := parts[1]

	// Read and discard headers
	for {
		line, err := reader.ReadString('\n')
		if err != nil || line == "\r\n" {
			break
		}
	}

	// Only handle GET requests
	if method != "GET" {
		sendError(conn, 405, "Method Not Allowed")
		return
	}

	// Serve file
	serveFile(conn, path)
}

func serveFile(conn net.Conn, urlPath string) {
	// Security: prevent path traversal attacks
	// Clean the path and ensure it's within ROOT_DIR
	cleanPath := filepath.Clean(urlPath)
	if strings.Contains(cleanPath, "..") {
		sendError(conn, 403, "Forbidden")
		return
	}

	// Map URL to filesystem
	if urlPath == "/" {
		urlPath = "/index.html"
	}

	filePath := filepath.Join(ROOT_DIR, urlPath)

	// Open file
	file, err := os.Open(filePath)
	if err != nil {
		sendError(conn, 404, "Not Found")
		return
	}
	defer file.Close()

	// Get file info
	stat, err := file.Stat()
	if err != nil {
		sendError(conn, 500, "Internal Server Error")
		return
	}

	// Determine content type
	contentType := "application/octet-stream"
	if strings.HasSuffix(filePath, ".html") {
		contentType = "text/html"
	} else if strings.HasSuffix(filePath, ".css") {
		contentType = "text/css"
	} else if strings.HasSuffix(filePath, ".js") {
		contentType = "application/javascript"
	} else if strings.HasSuffix(filePath, ".json") {
		contentType = "application/json"
	}

	// Send response headers
	header := fmt.Sprintf(
		"HTTP/1.1 200 OK\r\n"+
			"Content-Type: %s\r\n"+
			"Content-Length: %d\r\n"+
			"\r\n",
		contentType, stat.Size(),
	)
	conn.Write([]byte(header))

	// Zero-copy file transfer
	// io.Copy uses sendfile() syscall when possible
	io.Copy(conn, file)
}

func sendError(conn net.Conn, status int, message string) {
	statusText := map[int]string{
		400: "Bad Request",
		403: "Forbidden",
		404: "Not Found",
		405: "Method Not Allowed",
		500: "Internal Server Error",
	}

	body := fmt.Sprintf("<h1>%d %s</h1><p>%s</p>", status, statusText[status], message)

	response := fmt.Sprintf(
		"HTTP/1.1 %d %s\r\n"+
			"Content-Type: text/html\r\n"+
			"Content-Length: %d\r\n"+
			"\r\n"+
			"%s",
		status, statusText[status], len(body), body,
	)

	conn.Write([]byte(response))
}
