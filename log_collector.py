import socket
import sys
import logging
import json
import argparse
from select import poll, POLLIN, POLLERR  # poll() system call 

BUFFER_SIZE = 512
LOG_FILE = "proxy_logs.jsonl"

logging.basicConfig(level=logging.DEBUG, 
                    format='%(levelname)s - %(message)s')

def parse_arguments():
    parser = argparse.ArgumentParser(description="Log collector to aggregate client, server, and proxy logs")
    parser.add_argument('--listen-ip',
                        required=True,
                        help="IP address to bind to")
    parser.add_argument('--listen-port',
                        type=int,
                        required=True,
                        help="Port to listen on")
    
    args = parser.parse_args()
    
    # Port validation
    if not (0 < args.listen_port < 65536):
        logging.error(f"Invalid port: {args.listen_port}. Must be 1-65535.")
        sys.exit(1)
        
    return args.listen_ip, args.listen_port


def create_socket(ip_addr):
    # Determine if IPv4 or IPv6
    try:
        socket.inet_pton(socket.AF_INET, ip_addr)
        family = socket.AF_INET
    except socket.error:
        try:
            socket.inet_pton(socket.AF_INET6, ip_addr)
            family = socket.AF_INET6
        except socket.error:
            logging.error(f"{ip_addr} is not a valid IPv4 or IPv6 address")
            sys.exit(1)
    
    # Create socket
    try:
        sock = socket.socket(family, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return sock
    except socket.error as e:
        logging.error(f"Error creating socket: {e}")
        sys.exit(1)


def bind_socket(sock: socket.socket, ip_addr, port):
    try:
        sock.bind((ip_addr, port))
        logging.debug(f"Bound to socket: {ip_addr}:{port}")
    except socket.error as e:
        logging.debug(f"socket: {ip_addr}:{port}")
        logging.error(f"Binding failed: {e}")
        sys.exit(1)


def listen_for_connections(sock):
    try:
        sock.listen(1)
        logging.debug("Listening for connections on socket")
    except socket.error as e:
        logging.error(f"Error listening to socket: {e}")
        sys.exit(1)


def handle_log(clientfd, log_file):
    try:
        
        data = clientfd.recv(BUFFER_SIZE)
        if not data:
            return False # Disconnect
        
        buffer = data.decode('utf-8')
        logs = buffer.split("<14>")

        for log in logs:
            if not log:
                continue
            try:
                log_entry = json.loads(log)
                print(f"{log_entry}")
                log_file.write(log + "\n")
                log_file.flush()
            except json.JSONDecodeError:
                logging.error(f"Bad JSON: {log}")
                                                           
        return True

    except Exception as e:
        logging.error(f"Read error: {e}")
        return False

def run_server(sock):
    poller = poll()
    poller.register(sock.fileno(), POLLIN)
    connections = {sock.fileno(): sock}
    log_file = open(LOG_FILE, "a", encoding="utf-8")

    logging.info("Collector running. Press CTRL+C to stop.")
    try:
        while True:
            client_list = poller.poll(1000)

            for fd, event in client_list:
                logging.debug(f"Client: {fd} Event: {event}")
                if fd == sock.fileno() and event == POLLIN: # server
                    try: 
                        clientfd, client_addr = sock.accept()
                        clientfd.setblocking(False)
                        logging.info(f"Logger connected from {client_addr}")

                        poller.register(clientfd.fileno(), POLLIN)
                        connections[clientfd.fileno()] = clientfd
                    except socket.error as e:
                        logging.error(f"Error accepting connection: {e}")
                        continue

                elif fd and event == POLLIN: # read logs
                    clientfd = connections[fd]
                    message = handle_log(clientfd, log_file)
                    if not message:
                        logging.info(f"Closing connection {fd}")
                        poller.unregister(fd)
                        clientfd.close()
                        del connections[fd]
                
                elif fd and event == POLLERR:
                    logging.info(f"Hangup/Error on fd {fd}")
                    poller.unregister(fd)
                    connections[fd].close()
                    del connections[fd]
    except KeyboardInterrupt:
        print("\nCTRL+C detected. Shutting down...")
    finally:
        log_file.close()

        open(LOG_FILE, "w",).close() # Empties file
        
        for fd, sock in connections.items():
            sock.close()
        print("Cleanup complete.")

def main():
    ip_addr, port = parse_arguments()
    sockfd = create_socket(ip_addr)
    bind_socket(sockfd, ip_addr, port)
    listen_for_connections(sockfd)
        
    run_server(sockfd)
    

if __name__ == "__main__":
    main()