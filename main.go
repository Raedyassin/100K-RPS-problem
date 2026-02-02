package main

import (
	"fmt"
	"net"
	"os"
)

func main() {
	// Listen on port 8080
	// "tcp" = protocol, ":8080" = listen on all interfaces, port 8080
	listener, err := net.Listen("tcp", ":8080")
	if err != nil {
		fmt.Println("Error starting server:", err)
		os.Exit(1)
	}
	defer listener.Close() // Close when main() exits

	fmt.Println("Server listening on :8080")

	// Accept connections in a loop
	for {
		// This BLOCKS until a client connects
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err)
			continue
		}

		// Handle this connection
		handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close() // Close connection when done

	// Read data from client
	buffer := make([]byte, 1024) // Create a 1KB buffer
	n, err := conn.Read(buffer)  // Read into buffer
	if err != nil {
		fmt.Println("Error reading:", err)
		return
	}

	fmt.Printf("Received: %s\n", buffer[:n])

	// Send response
	response := "HTTP/1.1 200 OK\r\n\r\nHello World"
	conn.Write([]byte(response))
}
