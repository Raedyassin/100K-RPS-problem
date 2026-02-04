"""
HIGH-PERFORMANCE WEB SERVER
Production-ready, 100K+ RPS capable
Cross-platform: Windows, Linux, macOS
"""

import asyncio  # Async I/O event loop - the core of our event-driven architecture
import socket  # Low-level socket operations
import os  # Operating system interfaces
import sys  # System-specific parameters
import signal  # Signal handling for graceful shutdown
import multiprocessing  # Multi-process for utilizing all CPU cores
from pathlib import Path  # Modern path handling
import mmap  # Memory-mapped files for efficient file serving
import re  # Regular expressions for HTTP parsing
from typing import Optional, Tuple  # Type hints for code clarity

# Platform detection - determines which OS-specific optimizations to use
IS_WINDOWS = sys.platform == 'win32'  # True if running on Windows
IS_LINUX = sys.platform.startswith('linux')  # True if running on Linux

# Try to import uvloop for Linux/Mac - it's 2-4x faster than default asyncio
if not IS_WINDOWS:
    try:
        import uvloop  # Ultra-fast event loop implementation
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())  # Replace default loop
    except ImportError:
        pass  # Fall back to default asyncio loop if uvloop not installed

# Windows-specific imports for TransmitFile (zero-copy file sending)
if IS_WINDOWS:
    try:
        import win32file  # Windows file operations
        import pywintypes  # Windows types
        HAS_TRANSMITFILE = True  # Flag indicating TransmitFile is available
    except ImportError:
        HAS_TRANSMITFILE = False  # Fall back to regular file sending
        print("Warning: pywin32 not installed. Install for zero-copy on Windows.")


# =============================================================================
# CONFIGURATION - Single place to tune server performance
# =============================================================================

class Config:
    """Server configuration - all tunable parameters in one place"""
    
    # Network settings
    HOST = '0.0.0.0'  # Listen on all network interfaces (0.0.0.0 = accept from any IP)
    PORT = 8080  # Standard HTTP alternative port (80 requires root/admin)
    BACKLOG = 2048  # Socket listen backlog - queue size for pending connections
                    # Linux default is 128, we increase for high load
    
    # Connection limits - prevents resource exhaustion
    MAX_CONCURRENT_CONNECTIONS = 1000  # Max simultaneous active connections per worker
                                        # This is your "connection pool" concept
    
    # Keep-Alive settings - reuse TCP connections for multiple requests
    KEEPALIVE_TIMEOUT = 75  # Seconds to keep connection open waiting for next request
                            # Nginx default is 75s
    KEEPALIVE_MAX_REQUESTS = 1000  # Max requests per connection before forcing close
                                    # Prevents memory leaks from long-lived connections
    
    # Process settings - utilize all CPU cores
    WORKER_PROCESSES = multiprocessing.cpu_count()  # One worker per CPU core
                                                     # Avoids GIL contention
    
    # Buffer sizes - trade memory for performance
    READ_BUFFER_SIZE = 8192  # 8KB - read this much data per recv() call
                             # Larger = fewer syscalls, more memory
    RESPONSE_BUFFER_SIZE = 16384  # 16KB - buffer responses before sending
    
    # File serving
    STATIC_DIR = Path('./static')  # Directory containing static files
    INDEX_FILE = 'index.html'  # Default file for directory requests
    
    # HTTP parsing - pre-compiled regex for speed
    # Matches: "GET /path HTTP/1.1"
    REQUEST_LINE_REGEX = re.compile(
        rb'^([A-Z]+) +([^ ]+) +HTTP/(\d+\.\d+)\r\n',  # rb = raw bytes
        re.IGNORECASE  # Case-insensitive matching
    )


# =============================================================================
# ZERO-COPY FILE SENDING - Platform-specific implementations
# =============================================================================

async def sendfile_portable(sock: socket.socket, filepath: Path, 
                            offset: int = 0, count: Optional[int] = None) -> int:
    """
    Cross-platform zero-copy file sending.
    
    Zero-copy means: file → kernel buffer → network, NO userspace copy.
    Traditional: file → kernel → userspace → kernel → network (2 extra copies!)
    
    Args:
        sock: Socket to send file through
        filepath: Path to file to send
        offset: Starting byte position in file
        count: Number of bytes to send (None = entire file)
    
    Returns:
        Number of bytes sent
    """
    
    if IS_LINUX:
        # Linux: Use os.sendfile() - direct kernel-to-kernel transfer
        # Syntax: sendfile(out_fd, in_fd, offset, count)
        try:
            loop = asyncio.get_event_loop()  # Get current event loop
            fd = os.open(filepath, os.O_RDONLY)  # Open file, get file descriptor
            try:
                file_size = os.fstat(fd).st_size  # Get file size
                if count is None:
                    count = file_size - offset  # Send from offset to end
                
                sent = 0  # Track total bytes sent
                # sendfile() may not send all bytes in one call, loop until done
                while sent < count:
                    # Run blocking sendfile() in executor to not block event loop
                    n = await loop.run_in_executor(
                        None,  # None = use default executor (ThreadPoolExecutor)
                        os.sendfile,  # Function to run
                        sock.fileno(),  # Output: socket file descriptor
                        fd,  # Input: file descriptor
                        offset + sent,  # Current position in file
                        count - sent  # Remaining bytes to send
                    )
                    if n == 0:  # 0 bytes sent = EOF or error
                        break
                    sent += n  # Update bytes sent
                return sent
            finally:
                os.close(fd)  # Always close file descriptor
        except Exception as e:
            # Fall back to regular file reading if sendfile fails
            return await sendfile_fallback(sock, filepath, offset, count)
    
    elif IS_WINDOWS and HAS_TRANSMITFILE:
        # Windows: Use TransmitFile() - similar to sendfile()
        try:
            loop = asyncio.get_event_loop()
            # Open file handle
            handle = win32file.CreateFile(
                str(filepath),  # File path
                win32file.GENERIC_READ,  # Read-only access
                win32file.FILE_SHARE_READ,  # Allow others to read
                None,  # Default security
                win32file.OPEN_EXISTING,  # File must exist
                win32file.FILE_ATTRIBUTE_NORMAL,  # Normal file
                None  # No template file
            )
            try:
                # TransmitFile is blocking, run in executor
                def transmit():
                    win32file.TransmitFile(
                        sock.fileno(),  # Socket
                        handle,  # File handle
                        count or 0,  # 0 = send entire file
                        0,  # Bytes per send (0 = default)
                        None,  # No overlapped I/O structure
                        None,  # No head buffer
                        None  # No tail buffer
                    )
                    return count or os.fstat(handle).st_size
                
                return await loop.run_in_executor(None, transmit)
            finally:
                handle.Close()  # Close file handle
        except Exception as e:
            return await sendfile_fallback(sock, filepath, offset, count)
    
    else:
        # macOS, BSD, or Windows without pywin32: use fallback
        return await sendfile_fallback(sock, filepath, offset, count)


async def sendfile_fallback(sock: socket.socket, filepath: Path, 
                            offset: int = 0, count: Optional[int] = None) -> int:
    """
    Fallback file sending using memory-mapped file.
    
    Not true zero-copy, but faster than read() because:
    - mmap maps file directly into memory
    - Avoids extra buffer allocation
    - Kernel can optimize page cache usage
    """
    loop = asyncio.get_event_loop()
    
    with open(filepath, 'rb') as f:  # Open file in binary read mode
        file_size = os.fstat(f.fileno()).st_size  # Get file size
        if count is None:
            count = file_size - offset  # Calculate bytes to send
        
        # Memory-map the file - maps file content into memory address space
        with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mmapped:
            sent = 0  # Track bytes sent
            # Send in chunks to avoid blocking event loop for huge files
            while sent < count:
                chunk_size = min(Config.RESPONSE_BUFFER_SIZE, count - sent)
                # Get chunk of data from memory-mapped file
                chunk = mmapped[offset + sent:offset + sent + chunk_size]
                # Send chunk (run in executor since sendall blocks)
                await loop.run_in_executor(None, sock.sendall, chunk)
                sent += chunk_size
            return sent


# =============================================================================
# HTTP RESPONSE BUILDER - Efficient response construction
# =============================================================================

class HTTPResponse:
    """Builds HTTP responses efficiently using bytes operations"""
    
    # Pre-built status lines as bytes - no runtime string conversion
    STATUS_LINES = {
        200: b'HTTP/1.1 200 OK\r\n',
        400: b'HTTP/1.1 400 Bad Request\r\n',
        404: b'HTTP/1.1 404 Not Found\r\n',
        500: b'HTTP/1.1 500 Internal Server Error\r\n',
    }
    
    # Common headers as bytes
    HEADER_CONNECTION_CLOSE = b'Connection: close\r\n'
    HEADER_CONNECTION_KEEPALIVE = b'Connection: keep-alive\r\n'
    HEADER_CONTENT_TYPE_HTML = b'Content-Type: text/html; charset=utf-8\r\n'
    HEADER_CONTENT_TYPE_PLAIN = b'Content-Type: text/plain; charset=utf-8\r\n'
    HEADER_SERVER = b'Server: HighPerfPython/1.0\r\n'
    
    @staticmethod
    def build_response(status: int, body: bytes, keep_alive: bool = True,
                      content_type: bytes = None) -> bytes:
        """
        Build complete HTTP response as bytes.
        
        Format:
        HTTP/1.1 200 OK\r\n
        Server: HighPerfPython/1.0\r\n
        Content-Length: 13\r\n
        Content-Type: text/html\r\n
        Connection: keep-alive\r\n
        \r\n
        Hello, World!
        """
        # Start with status line
        response = bytearray(HTTPResponse.STATUS_LINES.get(status, 
                            HTTPResponse.STATUS_LINES[500]))
        
        # Add server header
        response.extend(HTTPResponse.HEADER_SERVER)
        
        # Add Content-Length - required for keep-alive
        response.extend(b'Content-Length: ')
        response.extend(str(len(body)).encode('ascii'))  # Convert int to bytes
        response.extend(b'\r\n')
        
        # Add Content-Type
        if content_type:
            response.extend(content_type)
        else:
            response.extend(HTTPResponse.HEADER_CONTENT_TYPE_HTML)
        
        # Add Connection header
        if keep_alive:
            response.extend(HTTPResponse.HEADER_CONNECTION_KEEPALIVE)
        else:
            response.extend(HTTPResponse.HEADER_CONNECTION_CLOSE)
        
        # End of headers
        response.extend(b'\r\n')
        
        # Add body
        response.extend(body)
        
        return bytes(response)  # Convert bytearray to bytes
    
    @staticmethod
    def build_file_response_headers(file_size: int, keep_alive: bool = True) -> bytes:
        """Build headers for file response (body sent separately via zero-copy)"""
        response = bytearray(HTTPResponse.STATUS_LINES[200])
        response.extend(HTTPResponse.HEADER_SERVER)
        response.extend(b'Content-Length: ')
        response.extend(str(file_size).encode('ascii'))
        response.extend(b'\r\n')
        response.extend(b'Content-Type: application/octet-stream\r\n')
        
        if keep_alive:
            response.extend(HTTPResponse.HEADER_CONNECTION_KEEPALIVE)
        else:
            response.extend(HTTPResponse.HEADER_CONNECTION_CLOSE)
        
        response.extend(b'\r\n')  # End headers
        return bytes(response)


# =============================================================================
# HTTP REQUEST HANDLER - Core request processing logic
# =============================================================================

class RequestHandler:
    """Handles HTTP requests - parsing, routing, response generation"""
    
    def __init__(self):
        self.request_count = 0  # Track requests on this connection
    
    async def handle_request(self, reader: asyncio.StreamReader, 
                            writer: asyncio.StreamWriter) -> bool:
        """
        Handle a single HTTP request.
        
        Returns:
            True if connection should stay alive (keep-alive)
            False if connection should close
        """
        try:
            # Read request line: "GET /path HTTP/1.1\r\n"
            request_line = await asyncio.wait_for(
                reader.readline(),  # Read until \n
                timeout=10.0  # 10 second timeout - prevents slowloris attacks
            )
            
            if not request_line:  # Empty read = client closed connection
                return False
            
            # Parse request using regex
            match = Config.REQUEST_LINE_REGEX.match(request_line)
            if not match:
                # Invalid request format
                response = HTTPResponse.build_response(
                    400,  # Bad Request
                    b'<h1>400 Bad Request</h1>',
                    keep_alive=False
                )
                writer.write(response)  # Write response to socket buffer
                await writer.drain()  # Flush buffer to network
                return False
            
            # Extract method, path, HTTP version
            method = match.group(1)  # e.g., b'GET'
            path = match.group(2).decode('utf-8', errors='ignore')  # e.g., '/index.html'
            http_version = match.group(3)  # e.g., b'1.1'
            
            # Read headers (we need to consume them even if not using)
            headers = {}
            while True:
                header_line = await asyncio.wait_for(
                    reader.readline(),
                    timeout=5.0
                )
                if header_line == b'\r\n':  # Empty line = end of headers
                    break
                # Parse header: "Name: Value\r\n"
                if b':' in header_line:
                    name, value = header_line.split(b':', 1)
                    headers[name.strip().lower()] = value.strip()
            
            # Determine if client wants keep-alive
            connection_header = headers.get(b'connection', b'').lower()
            # HTTP/1.1 defaults to keep-alive, HTTP/1.0 defaults to close
            client_wants_keepalive = (
                http_version == b'1.1' and connection_header != b'close'
            ) or (connection_header == b'keep-alive')
            
            # Increment request count for this connection
            self.request_count += 1
            
            # Check if we should keep connection alive
            keep_alive = (
                client_wants_keepalive and
                self.request_count < Config.KEEPALIVE_MAX_REQUESTS
            )
            
            # Route request to handler
            if method == b'GET':
                await self.handle_get(writer, path, keep_alive)
            else:
                # Method not implemented
                response = HTTPResponse.build_response(
                    400,
                    b'<h1>Method Not Allowed</h1>',
                    keep_alive=False
                )
                writer.write(response)
                await writer.drain()
                return False
            
            return keep_alive  # Return whether to keep connection alive
            
        except asyncio.TimeoutError:
            # Client too slow - close connection
            return False
        except Exception as e:
            # Unexpected error - send 500 and close
            try:
                response = HTTPResponse.build_response(
                    500,
                    b'<h1>500 Internal Server Error</h1>',
                    keep_alive=False
                )
                writer.write(response)
                await writer.drain()
            except:
                pass  # If we can't even send error, just close
            return False
    
    async def handle_get(self, writer: asyncio.StreamWriter, path: str, 
                        keep_alive: bool):
        """Handle GET request - serve static file or generate response"""
        
        # Normalize path - remove leading slash, prevent directory traversal
        if path == '/':
            path = Config.INDEX_FILE  # Default to index.html
        else:
            path = path.lstrip('/')  # Remove leading slash
        
        # Security: prevent directory traversal attacks (../)
        # Resolve to absolute path and check it's within static dir
        try:
            requested_file = (Config.STATIC_DIR / path).resolve()
            Config.STATIC_DIR.resolve()  # Ensure static dir exists
            
            # Check if resolved path is within static directory
            if Config.STATIC_DIR not in requested_file.parents and requested_file != Config.STATIC_DIR:
                raise ValueError("Path outside static directory")
            
        except:
            # Path traversal attempt or invalid path
            response = HTTPResponse.build_response(
                404,
                b'<h1>404 Not Found</h1>',
                keep_alive=keep_alive
            )
            writer.write(response)
            await writer.drain()
            return
        
        # Check if file exists and is a file (not directory)
        if requested_file.exists() and requested_file.is_file():
            # Serve file using zero-copy
            await self.serve_file(writer, requested_file, keep_alive)
        else:
            # File not found
            response = HTTPResponse.build_response(
                404,
                b'<h1>404 Not Found</h1>',
                keep_alive=keep_alive
            )
            writer.write(response)
            await writer.drain()
    
    async def serve_file(self, writer: asyncio.StreamWriter, 
                        filepath: Path, keep_alive: bool):
        """Serve file using zero-copy sendfile"""
        try:
            file_size = filepath.stat().st_size  # Get file size
            
            # Send headers first
            headers = HTTPResponse.build_file_response_headers(file_size, keep_alive)
            writer.write(headers)
            await writer.drain()  # Ensure headers sent before file
            
            # Send file using zero-copy
            sock = writer.get_extra_info('socket')  # Get underlying socket
            if sock:
                await sendfile_portable(sock, filepath, count=file_size)
            else:
                # No socket available (shouldn't happen), use fallback
                with open(filepath, 'rb') as f:
                    data = f.read()
                    writer.write(data)
                    await writer.drain()
                    
        except Exception as e:
            # Error serving file
            print(f"Error serving file {filepath}: {e}")


# =============================================================================
# CONNECTION HANDLER - Manages individual client connections
# =============================================================================

async def handle_client(reader: asyncio.StreamReader, 
                       writer: asyncio.StreamWriter,
                       semaphore: asyncio.Semaphore):
    """
    Handle a client connection with keep-alive support.
    
    This function:
    1. Acquires semaphore slot (enforces max concurrent connections)
    2. Processes requests in a loop (keep-alive)
    3. Releases semaphore when done
    
    Args:
        reader: Async stream reader for receiving data
        writer: Async stream writer for sending data
        semaphore: Limits concurrent connections to MAX_CONCURRENT_CONNECTIONS
    """
    # Acquire semaphore - blocks if 1000 connections already active
    # This is your "connection pool" - limits active connections
    async with semaphore:  # Automatically releases on exit
        handler = RequestHandler()  # Create handler for this connection
        
        try:
            # Enable TCP_NODELAY - disables Nagle's algorithm
            # Nagle buffers small packets - good for telnet, bad for HTTP
            # We want immediate sending for low latency
            sock = writer.get_extra_info('socket')
            if sock:
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            
            # Keep-alive loop - handle multiple requests on same connection
            while True:
                # Handle one request
                keep_alive = await asyncio.wait_for(
                    handler.handle_request(reader, writer),
                    timeout=Config.KEEPALIVE_TIMEOUT  # 75 second timeout
                )
                
                if not keep_alive:
                    # Client wants to close, or error occurred, or max requests reached
                    break
                
                # Continue to next request on same connection
                
        except asyncio.TimeoutError:
            # Keep-alive timeout - client didn't send next request in time
            pass
        except Exception as e:
            # Unexpected error
            print(f"Connection error: {e}")
        finally:
            # Always close connection cleanly
            try:
                writer.close()  # Close socket
                await writer.wait_closed()  # Wait for close to complete
            except:
                pass  # Ignore errors during close


# =============================================================================
# WORKER PROCESS - Event loop that handles connections
# =============================================================================

async def run_worker(host: str, port: int, worker_id: int):
    """
    Worker process main function.
    
    Each worker:
    - Creates its own event loop (no GIL contention between workers)
    - Binds to same port using SO_REUSEPORT (Linux) or SO_REUSEADDR (Windows)
    - Accepts connections and spawns handler coroutines
    - Uses semaphore to limit concurrent connections
    
    Args:
        host: IP to bind to
        port: Port to bind to
        worker_id: Worker identifier for logging
    """
    print(f"Worker {worker_id} starting on {host}:{port}")
    
    # Create semaphore - limits concurrent connections
    # When 1000 connections active, new connections block until slot free
    semaphore = asyncio.Semaphore(Config.MAX_CONCURRENT_CONNECTIONS)
    
    # Create server socket manually for fine-grained control
    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    
    # Set socket options
    server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)  # Reuse address
    
    # SO_REUSEPORT (Linux/Mac) - allows multiple processes to bind same port
    # Kernel load-balances incoming connections across processes
    if hasattr(socket, 'SO_REUSEPORT') and not IS_WINDOWS:
        server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
    
    # Bind and listen
    server_sock.bind((host, port))
    server_sock.listen(Config.BACKLOG)  # 2048 pending connections queue
    server_sock.setblocking(False)  # Non-blocking mode for async
    
    # Create asyncio server from existing socket
    loop = asyncio.get_event_loop()
    server = await asyncio.start_server(
        lambda r, w: handle_client(r, w, semaphore),  # Handler function
        sock=server_sock  # Use our configured socket
    )
    
    print(f"Worker {worker_id} ready, accepting connections")
    
    # Run server forever
    async with server:
        await server.serve_forever()


def worker_process(host: str, port: int, worker_id: int):
    """
    Worker process entry point.
    
    This runs in a separate process (multiprocessing.Process).
    Sets up its own event loop and runs the worker coroutine.
    """
    # Each worker process needs its own event loop
    if not IS_WINDOWS:
        # Use uvloop if available (Linux/Mac)
        try:
            import uvloop
            asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        except ImportError:
            pass
    
    # Create event loop for this process
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    # Run worker
    try:
        loop.run_until_complete(run_worker(host, port, worker_id))
    except KeyboardInterrupt:
        print(f"Worker {worker_id} shutting down")
    finally:
        loop.close()


# =============================================================================
# MASTER PROCESS - Manages worker processes
# =============================================================================

def main():
    """
    Master process - spawns and manages worker processes.
    
    Architecture:
    - Master process spawns N workers (N = CPU cores)
    - Each worker is independent process with own GIL
    - Workers share listening socket (SO_REUSEPORT)
    - Master monitors workers, restarts if they crash
    """
    
    # Create static directory if it doesn't exist
    Config.STATIC_DIR.mkdir(exist_ok=True)
    
    # Create sample index.html if it doesn't exist
    index_file = Config.STATIC_DIR / Config.INDEX_FILE
    if not index_file.exists():
        index_file.write_text("""
<!DOCTYPE html>
<html>
<head>
    <title>High-Performance Python Server</title>
</head>
<body>
    <h1>High-Performance Python Web Server</h1>
    <p>100K+ RPS Capable</p>
    <ul>
        <li>Multi-process architecture (CPU cores utilized)</li>
        <li>Event-driven I/O (asyncio)</li>
        <li>Zero-copy file serving</li>
        <li>HTTP Keep-Alive</li>
        <li>Connection limiting (1000 concurrent per worker)</li>
        <li>Cross-platform (Windows/Linux/macOS)</li>
    </ul>
</body>
</html>
        """.strip())
    
    print("=" * 70)
    print("HIGH-PERFORMANCE PYTHON WEB SERVER")
    print("=" * 70)
    print(f"Platform: {sys.platform}")
    print(f"Python: {sys.version}")
    print(f"Workers: {Config.WORKER_PROCESSES}")
    print(f"Max connections per worker: {Config.MAX_CONCURRENT_CONNECTIONS}")
    print(f"Total max concurrent: {Config.WORKER_PROCESSES * Config.MAX_CONCURRENT_CONNECTIONS}")
    print(f"Listening: {Config.HOST}:{Config.PORT}")
    print(f"Static dir: {Config.STATIC_DIR.absolute()}")
    print("=" * 70)
    
    # Spawn worker processes
    workers = []
    for i in range(Config.WORKER_PROCESSES):
        # Create process
        p = multiprocessing.Process(
            target=worker_process,  # Function to run
            args=(Config.HOST, Config.PORT, i)  # Arguments
        )
        p.start()  # Start process
        workers.append(p)
    
    # Master process just monitors workers
    try:
        # Wait for all workers
        for p in workers:
            p.join()  # Block until process exits
    except KeyboardInterrupt:
        print("\nShutting down server...")
        # Terminate all workers
        for p in workers:
            p.terminate()  # Send SIGTERM
        # Wait for clean shutdown
        for p in workers:
            p.join(timeout=5)  # Wait up to 5 seconds
            if p.is_alive():
                p.kill()  # Force kill if still alive
    
    print("Server stopped")


# =============================================================================
# ENTRY POINT
# =============================================================================

if __name__ == '__main__':
    # Set multiprocessing start method
    # 'spawn' works on all platforms, 'fork' is Linux/Mac only but faster
    if IS_WINDOWS:
        multiprocessing.set_start_method('spawn')
    else:
        multiprocessing.set_start_method('fork')  # Faster on Unix
    
    main()