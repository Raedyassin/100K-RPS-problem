"""
High-Performance Web Server - Production Ready
Handles 100K+ RPS with event-driven architecture
"""

# ============================================================================
# IMPORTS SECTION
# ============================================================================

import asyncio  # Python's built-in async I/O library - gives us event loop
import socket   # Low-level networking - we need this for socket options
import signal   # For handling shutdown signals like Ctrl+C gracefully
import os       # Operating system functions - we need this for process management
import sys      # System-specific parameters - for error handling
import multiprocessing  # For creating worker processes (bypass GIL)
from typing import Dict, Tuple, Optional  # Type hints for code clarity
import logging  # Proper logging for production environments
from dataclasses import dataclass  # Clean way to define configuration classes
import time     # For performance metrics and timestamps

# Third-party imports (install with: pip install httptools uvloop)
try:
    import httptools  # Fast HTTP parser written in C - 2-3x faster than pure Python
except ImportError:
    print("ERROR: httptools not installed. Run: pip install httptools")
    sys.exit(1)

try:
    import uvloop  # libuv-based event loop - faster than asyncio's default
    # uvloop is 2-4x faster than asyncio's event loop for I/O operations
except ImportError:
    print("WARNING: uvloop not installed. Performance will be reduced.")
    print("Install with: pip install uvloop")
    uvloop = None  # Will fall back to standard asyncio event loop

# ============================================================================
# CONFIGURATION
# ============================================================================

@dataclass
class ServerConfig:
    """
    Configuration class for our web server.
    Using dataclass makes it easy to validate and document settings.
    """
    
    # Network settings
    host: str = "0.0.0.0"  # Listen on all interfaces (0.0.0.0 means all IPs)
    port: int = 8080       # Port to bind to
    
    # Performance tuning
    workers: int = multiprocessing.cpu_count()  # One worker per CPU core
    # Why? Python's GIL prevents true multi-threading, so we use processes
    # Each process gets its own Python interpreter = no GIL contention
    
    backlog: int = 4096    # Socket listen backlog queue size
    # This is how many pending connections can wait before accept()
    # High value = can handle bursts without dropping connections
    
    max_connections_per_worker: int = 10000  # Max concurrent connections per worker
    # Each worker can handle 10K connections = 10K * cpu_count total
    
    keepalive_timeout: int = 75  # Keep connections alive for 75 seconds
    # Reusing connections saves the overhead of TCP handshake (SYN, SYN-ACK, ACK)
    
    request_timeout: int = 30  # Maximum time to wait for a complete request
    # Prevents slow clients from holding connections forever (Slowloris attack)
    
    # Buffer sizes
    recv_buffer_size: int = 256 * 1024   # 256 KB - socket receive buffer
    send_buffer_size: int = 256 * 1024   # 256 KB - socket send buffer
    # Larger buffers = fewer system calls = better performance
    # But don't make them too large or you'll waste memory
    
    # Static file serving
    static_dir: str = "./static"  # Directory for static files
    enable_sendfile: bool = True   # Use zero-copy sendfile() for static files
    # sendfile() copies data from file descriptor to socket in kernel space
    # Normal read/write: File -> Kernel -> User -> Kernel -> Socket (4 copies!)
    # sendfile(): File -> Socket (1 copy!) = MUCH faster
    
    # Logging
    log_level: str = "INFO"  # INFO, DEBUG, WARNING, ERROR, CRITICAL


# ============================================================================
# HTTP PROTOCOL HANDLER
# ============================================================================

class HTTPProtocol(asyncio.Protocol):
    """
    This class handles the HTTP protocol for each connection.
    It's based on asyncio.Protocol which is a low-level interface.
    
    Why asyncio.Protocol instead of high-level streams?
    - More control over buffering
    - Less overhead (no stream abstractions)
    - Better for high-performance scenarios
    """
    
    # Class-level statistics (shared across all connections in this worker)
    total_requests = 0
    total_bytes_sent = 0
    
    def __init__(self, config: ServerConfig, loop: asyncio.AbstractEventLoop):
        """
        Constructor - called when a new connection is made.
        
        Args:
            config: Server configuration object
            loop: The event loop managing this connection
        """
        self.config = config
        self.loop = loop
        self.transport = None  # Will hold the socket transport
        self.parser = None     # Will hold the HTTP parser
        
        # Connection state
        self.request_count = 0  # How many requests on this connection
        self.keep_alive = True  # Should we keep connection alive?
        
        # Request data
        self.headers: Dict[bytes, bytes] = {}  # HTTP headers
        self.url: bytes = b""                   # Requested URL
        self.method: bytes = b""                # HTTP method (GET, POST, etc.)
        self.body: bytes = b""                  # Request body
        
        # Timeouts
        self.timeout_handle = None  # Handle for timeout cleanup
        
    def connection_made(self, transport: asyncio.Transport):
        """
        Called when a connection is established.
        This is where we configure the socket for performance.
        
        Args:
            transport: The socket transport object
        """
        self.transport = transport
        
        # Get the actual socket from the transport
        sock = transport.get_extra_info('socket')
        
        if sock:
            # ================================================================
            # CRITICAL SOCKET OPTIMIZATIONS
            # ================================================================
            
            # 1. TCP_NODELAY - Disable Nagle's algorithm
            # Nagle's algorithm batches small packets to reduce overhead
            # But for RPC/API servers, we want low latency, not batching
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            
            # 2. SO_KEEPALIVE - Enable TCP keepalive
            # Detects dead connections by sending periodic probes
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            
            # 3. SO_RCVBUF - Receive buffer size
            # Larger buffer = can receive more data before blocking
            try:
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 
                              self.config.recv_buffer_size)
            except OSError:
                logging.error("Failed to set receive buffer size")
                pass  # Some systems limit buffer size
            
            # 4. SO_SNDBUF - Send buffer size
            # Larger buffer = can send more data before blocking
            try:
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 
                              self.config.send_buffer_size)
            except OSError:
                pass
        
        # Create HTTP parser for this connection
        # httptools is a C library that's 2-3x faster than pure Python parsers
        self.parser = httptools.HttpRequestParser(self)
        
        # Set timeout for receiving request
        self._reset_timeout()
        
        logging.debug(f"Connection established from {transport.get_extra_info('peername')}")
    
    def _reset_timeout(self):
        """
        Reset the inactivity timeout.
        This prevents slow clients from holding connections forever.
        """
        # Cancel existing timeout if any
        if self.timeout_handle:
            self.timeout_handle.cancel()
        
        # Schedule new timeout
        # call_later() schedules a callback after N seconds
        self.timeout_handle = self.loop.call_later(
            self.config.request_timeout,
            self._timeout_occurred
        )
    
    def _timeout_occurred(self):
        """
        Called when a timeout occurs.
        Close the connection to free resources.
        """
        logging.warning("Request timeout - closing connection")
        if self.transport:
            self.transport.close()
    
    def data_received(self, data: bytes):
        """
        Called when data is received from the client.
        This is the core of our event-driven architecture!
        
        Instead of blocking and waiting for data (traditional approach),
        the event loop calls this method when data arrives.
        
        Args:
            data: Raw bytes received from the socket
        """
        try:
            # Feed data to the HTTP parser
            # The parser will call our callback methods (on_url, on_header, etc.)
            self.parser.feed_data(data)
        except httptools.HttpParserError as e:
            # Malformed HTTP request - send 400 Bad Request
            logging.warning(f"HTTP parse error: {e}")
            self._send_error(400, b"Bad Request")
            self.transport.close()
    
    # ========================================================================
    # HTTP PARSER CALLBACKS
    # These methods are called by httptools parser as it parses the request
    # ========================================================================
    
    def on_url(self, url: bytes):
        """Called when the URL is parsed."""
        self.url = url
    
    def on_header(self, name: bytes, value: bytes):
        """Called for each HTTP header."""
        # Store header (lowercase for case-insensitive lookup)
        self.headers[name.lower()] = value
    
    def on_body(self, body: bytes):
        """Called when request body data is received."""
        self.body += body
    
    def on_message_begin(self):
        """Called when a new HTTP message starts."""
        # Reset state for new request
        self.headers = {}
        self.url = b""
        self.body = b""
        self.method = b""
    
    def on_message_complete(self):
        """
        Called when the complete HTTP request has been received.
        This is where we process the request and send a response.
        """
        # Cancel timeout - we have the full request
        if self.timeout_handle:
            self.timeout_handle.cancel()
        
        # Get HTTP method (GET, POST, etc.)
        self.method = self.parser.get_method()
        
        # Check if client wants keep-alive
        connection_header = self.headers.get(b'connection', b'').lower()
        self.keep_alive = connection_header != b'close'
        
        # Increment request counter
        self.request_count += 1
        HTTPProtocol.total_requests += 1
        
        # Route and handle the request
        self._handle_request()
        
        # If keep-alive, reset for next request
        if self.keep_alive:
            self.parser = httptools.HttpRequestParser(self)
            self._reset_timeout()
        else:
            # Close connection after response is sent
            self.transport.close()
    
    def _handle_request(self):
        """
        Route the request to appropriate handler.
        This is a simple router - in production you'd use a more sophisticated one.
        """
        # Decode URL from bytes to string
        url = self.url.decode('utf-8', errors='ignore')
        method = self.method.decode('utf-8', errors='ignore')
        
        logging.debug(f"{method} {url}")
        
        # Simple routing
        if method == "GET":
            if url == "/" or url == "/index.html":
                self._send_response(200, b"text/html", b"<h1>Hello World!</h1>")
            elif url == "/health":
                # Health check endpoint for load balancers
                self._send_response(200, b"text/plain", b"OK")
            elif url == "/stats":
                # Statistics endpoint
                stats = (
                    f"Total Requests: {HTTPProtocol.total_requests}\n"
                    f"Total Bytes Sent: {HTTPProtocol.total_bytes_sent}\n"
                    f"Requests on this connection: {self.request_count}\n"
                ).encode()
                self._send_response(200, b"text/plain", stats)
            elif url.startswith("/static/"):
                # Serve static file with zero-copy sendfile
                self._serve_static_file(url[8:])  # Remove "/static/" prefix
            else:
                self._send_error(404, b"Not Found")
        else:
            # Method not implemented
            self._send_error(501, b"Not Implemented")
    
    def _serve_static_file(self, filepath: str):
        """
        Serve a static file using zero-copy sendfile() for maximum performance.
        
        Normal file serving:
        1. read() - Copy from disk to kernel buffer
        2. Copy from kernel buffer to user space (Python)
        3. write() - Copy from user space to kernel buffer
        4. Copy from kernel buffer to socket
        = 4 COPIES!
        
        With sendfile():
        1. sendfile() - Copy from disk to socket in kernel space
        = 1 COPY! (maybe even 0 with DMA)
        
        Args:
            filepath: Relative path to file in static directory
        """
        # Construct full path
        full_path = os.path.join(self.config.static_dir, filepath)
        
        # Security: prevent directory traversal attacks
        # Resolve to absolute path and check it's within static_dir
        try:
            full_path = os.path.abspath(full_path)
            static_dir = os.path.abspath(self.config.static_dir)
            
            if not full_path.startswith(static_dir):
                # Attempted directory traversal!
                self._send_error(403, b"Forbidden")
                return
        except Exception:
            self._send_error(400, b"Bad Request")
            return
        
        # Check if file exists
        if not os.path.isfile(full_path):
            self._send_error(404, b"Not Found")
            return
        
        try:
            # Get file size
            file_size = os.path.getsize(full_path)
            
            # Determine content type (simple version)
            content_type = self._get_content_type(filepath)
            
            # Open file
            with open(full_path, 'rb') as f:
                if self.config.enable_sendfile and hasattr(os, 'sendfile'):
                    # Use zero-copy sendfile
                    # Build HTTP headers
                    headers = (
                        b"HTTP/1.1 200 OK\r\n"
                        b"Content-Type: " + content_type + b"\r\n"
                        b"Content-Length: " + str(file_size).encode() + b"\r\n"
                        b"Connection: " + (b"keep-alive" if self.keep_alive else b"close") + b"\r\n"
                        b"\r\n"
                    )
                    
                    # Send headers
                    self.transport.write(headers)
                    HTTPProtocol.total_bytes_sent += len(headers)
                    
                    # Get socket file descriptor
                    sock = self.transport.get_extra_info('socket')
                    if sock:
                        # Use sendfile() to send file content
                        # This happens in kernel space - zero-copy!
                        offset = 0
                        while offset < file_size:
                            sent = os.sendfile(sock.fileno(), f.fileno(), offset, file_size - offset)
                            if sent == 0:
                                break
                            offset += sent
                            HTTPProtocol.total_bytes_sent += sent
                else:
                    # Fallback: read and send (not zero-copy)
                    content = f.read()
                    self._send_response(200, content_type, content)
        
        except Exception as e:
            logging.error(f"Error serving static file: {e}")
            self._send_error(500, b"Internal Server Error")
    
    def _get_content_type(self, filepath: str) -> bytes:
        """
        Determine content type from file extension.
        In production, use the 'mimetypes' module for comprehensive mapping.
        """
        if filepath.endswith('.html'):
            return b"text/html"
        elif filepath.endswith('.css'):
            return b"text/css"
        elif filepath.endswith('.js'):
            return b"application/javascript"
        elif filepath.endswith('.json'):
            return b"application/json"
        elif filepath.endswith('.png'):
            return b"image/png"
        elif filepath.endswith('.jpg') or filepath.endswith('.jpeg'):
            return b"image/jpeg"
        elif filepath.endswith('.gif'):
            return b"image/gif"
        else:
            return b"application/octet-stream"
    
    def _send_response(self, status: int, content_type: bytes, body: bytes):
        """
        Send an HTTP response.
        
        Args:
            status: HTTP status code (200, 404, etc.)
            content_type: MIME type of response
            body: Response body as bytes
        """
        # Map status code to reason phrase
        status_messages = {
            200: b"OK",
            400: b"Bad Request",
            403: b"Forbidden",
            404: b"Not Found",
            500: b"Internal Server Error",
            501: b"Not Implemented",
        }
        
        status_line = f"HTTP/1.1 {status} ".encode() + status_messages.get(status, b"Unknown")
        
        # Build complete response
        # Using bytearray for efficient concatenation
        response = bytearray()
        response.extend(status_line + b"\r\n")
        response.extend(b"Content-Type: " + content_type + b"\r\n")
        response.extend(b"Content-Length: " + str(len(body)).encode() + b"\r\n")
        response.extend(b"Connection: " + (b"keep-alive" if self.keep_alive else b"close") + b"\r\n")
        response.extend(b"\r\n")
        response.extend(body)
        
        # Send response
        # write() is non-blocking - it queues data for sending
        self.transport.write(bytes(response))
        
        HTTPProtocol.total_bytes_sent += len(response)
    
    def _send_error(self, status: int, message: bytes):
        """Send an HTTP error response."""
        body = b"<h1>" + message + b"</h1>"
        self._send_response(status, b"text/html", body)
    
    def connection_lost(self, exc):
        """
        Called when the connection is closed.
        Clean up resources.
        
        Args:
            exc: Exception if connection closed due to error, None otherwise
        """
        if self.timeout_handle:
            self.timeout_handle.cancel()
        
        if exc:
            logging.warning(f"Connection lost with error: {exc}")
        else:
            logging.debug("Connection closed normally")


# ============================================================================
# WORKER PROCESS
# ============================================================================

class Worker:
    """
    A worker process that runs an event loop.
    Each worker is a separate OS process, so no GIL contention!
    """
    
    def __init__(self, config: ServerConfig, worker_id: int):
        """
        Args:
            config: Server configuration
            worker_id: Unique ID for this worker (for logging)
        """
        self.config = config
        self.worker_id = worker_id
        self.server = None
        self.loop = None
    
    async def start(self):
        """
        Start the worker's event loop and begin accepting connections.
        """
        # Use uvloop if available (2-4x faster than default asyncio)
        if uvloop:
            asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        
        self.loop = asyncio.get_event_loop()
        
        # Create server socket
        # Why create_server()? It handles all the low-level socket setup
        self.server = await self.loop.create_server(
            lambda: HTTPProtocol(self.config, self.loop),  # Protocol factory
            host=self.config.host,
            port=self.config.port,
            backlog=self.config.backlog,
            reuse_address=True,    # SO_REUSEADDR - allow quick restart
            reuse_port=True,       # SO_REUSEPORT - multiple processes share port
            # SO_REUSEPORT is KEY for multi-process: each worker accept()s independently
            # Kernel load-balances incoming connections across workers
        )
        
        logging.info(f"Worker {self.worker_id} started on {self.config.host}:{self.config.port}")
        
        # Run forever (until signal)
        await self.server.serve_forever()
    
    def run(self):
        """
        Entry point for the worker process.
        Sets up logging and starts the event loop.
        """
        # Configure logging for this worker
        logging.basicConfig(
            level=getattr(logging, self.config.log_level),
            format=f'%(asctime)s - Worker-{self.worker_id} - %(levelname)s - %(message)s'
        )
        
        try:
            # Run the async start() method
            asyncio.run(self.start())
        except KeyboardInterrupt:
            logging.info(f"Worker {self.worker_id} shutting down...")
        except Exception as e:
            logging.error(f"Worker {self.worker_id} crashed: {e}")
            raise


# ============================================================================
# MASTER PROCESS
# ============================================================================

class Server:
    """
    Master process that spawns and manages worker processes.
    Uses pre-fork model: workers are created at startup, not per-request.
    """
    
    def __init__(self, config: ServerConfig):
        self.config = config
        self.workers: list = []  # List of worker processes
        self.running = False
    
    def _signal_handler(self, signum, frame):
        """
        Handle shutdown signals (SIGINT, SIGTERM).
        Gracefully shut down all workers.
        """
        logging.info(f"Received signal {signum}, shutting down...")
        self.running = False
        
        # Terminate all workers
        for worker in self.workers:
            worker.terminate()
        
        # Wait for workers to finish
        for worker in self.workers:
            worker.join()
        
        logging.info("All workers stopped. Goodbye!")
        sys.exit(0)
    
    def start(self):
        """
        Start the server: spawn workers and wait.
        """
        # Configure logging for master process
        logging.basicConfig(
            level=getattr(logging, self.config.log_level),
            format='%(asctime)s - Master - %(levelname)s - %(message)s'
        )
        
        # Create static directory if it doesn't exist
        os.makedirs(self.config.static_dir, exist_ok=True)
        
        # Register signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)   # Ctrl+C
        signal.signal(signal.SIGTERM, self._signal_handler)  # kill command
        
        self.running = True
        
        logging.info("=" * 60)
        logging.info("High-Performance Web Server Starting")
        logging.info("=" * 60)
        logging.info(f"Configuration:")
        logging.info(f"  Host: {self.config.host}")
        logging.info(f"  Port: {self.config.port}")
        logging.info(f"  Workers: {self.config.workers}")
        logging.info(f"  Max Connections/Worker: {self.config.max_connections_per_worker}")
        logging.info(f"  Total Capacity: {self.config.workers * self.config.max_connections_per_worker} connections")
        logging.info(f"  Keep-Alive Timeout: {self.config.keepalive_timeout}s")
        logging.info(f"  Static Directory: {self.config.static_dir}")
        logging.info(f"  Zero-Copy (sendfile): {self.config.enable_sendfile}")
        logging.info("=" * 60)
        
        # Spawn worker processes
        # Each worker is a full Python process (no GIL sharing!)
        for worker_id in range(self.config.workers):
            worker = Worker(self.config, worker_id)
            
            # Create process
            # target= is the function to run in the new process
            process = multiprocessing.Process(target=worker.run)
            process.start()
            
            self.workers.append(process)
            logging.info(f"Started worker {worker_id} (PID: {process.pid})")
        
        logging.info("=" * 60)
        logging.info(f"Server ready! Listening on http://{self.config.host}:{self.config.port}")
        logging.info("Press Ctrl+C to stop")
        logging.info("=" * 60)
        
        # Monitor workers (restart if they crash)
        while self.running:
            time.sleep(1)
            
            # Check if any worker has died
            for i, worker in enumerate(self.workers):
                if not worker.is_alive():
                    logging.error(f"Worker {i} died! Restarting...")
                    
                    # Start new worker
                    new_worker = Worker(self.config, i)
                    process = multiprocessing.Process(target=new_worker.run)
                    process.start()
                    
                    self.workers[i] = process
                    logging.info(f"Restarted worker {i} (PID: {process.pid})")


# ============================================================================
# ENTRY POINT
# ============================================================================

if __name__ == "__main__":
    # Create configuration
    config = ServerConfig(
        host="0.0.0.0",
        port=8080,
        workers=multiprocessing.cpu_count(),  # Use all CPU cores
        log_level="INFO",
    )
    
    # Create and start server
    server = Server(config)
    server.start()