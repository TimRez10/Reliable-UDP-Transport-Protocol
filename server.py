import socket
import sys
import signal
import argparse
import time
import json
from reliable_udp import ReliableUPD
from logger import setup_logging

BUFFER_SIZE = 512

console_logger, remote_logger = None, None

# Global socket for signal handler
sockfd = None

def parse_arguments():
    global console_logger, remote_logger
    parser = argparse.ArgumentParser(description="UDP server to display client messages")
    parser.add_argument('--listen-ip',
                        required=True,
                        help="IP address to bind to")
    parser.add_argument('--listen-port',
                        type=int,
                        required=True,
                        help="UDP port to listen on")
    parser.add_argument('--remote-log-ip',
                        default=None,
                        required=False,
                        help="IP address of remote log collector")
    parser.add_argument('--remote-log-port',
                        type=int,
                        default=None,
                        required=False,
                        help="Port of remote log collector")
    
    args = parser.parse_args()

    try:
        console_logger, remote_logger = setup_logging(args.remote_log_ip, args.remote_log_port)
    except Exception as e:
        print(f"Error setting up logging: {e}")
        sys.exit(1)

    # Validation
    if not (0 < args.listen_port < 65536):
        console_logger.error(f"Invalid port: {args.listen_port}. Must be 1-65535.")
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
            console_logger.error(f"{ip_addr} is not a valid IPv4 or IPv6 address")
            sys.exit(1)
    
    # Create socket
    try:
        sock = socket.socket(family, socket.SOCK_DGRAM)
        return sock
    except socket.error as e:
        console_logger.error(f"Error creating socket: {e}")
        sys.exit(1)


def bind_socket(sock: socket.socket, ip_addr, port):
    try:
        sock.bind((ip_addr, port))
        console_logger.debug(f"Bound to socket: {ip_addr}:{port}")
    except socket.error as e:
        console_logger.error(f"Binding failed: {e}")
        sys.exit(1)

def send_acknowledgement(sock: socket.socket, ack_num, client_address):
    try:
        ack_packet = ReliableUPD('', ack_num)
        sock.sendto(ack_packet.to_bytes(), client_address)
        log_payload = {
            'origin': 'SERVER',
            'timestamp': time.time(),
            'action': "ACK_SENT",
            'message': ack_packet.message,
            'seq-ack': ack_packet.seq_ack_num
        }
        remote_logger.info(json.dumps(log_payload))

    except socket.error as e:
        console_logger.error(f"Error sending ACK to {client_address}: {e}")
    except Exception as e:
        console_logger.error(f"Error in handle_client_request: {e}")


def handle_client_request(sock: socket.socket, packet: ReliableUPD, client_address):
    console_logger.debug(f"Received from {client_address}: '{packet.seq_ack_num}'")
    log_payload = {
        'origin': 'SERVER',
        'timestamp': time.time(),
        'action': "PACKET_RECEIVED",
        'message': packet.message,
        'seq-ack': packet.seq_ack_num
    }
    remote_logger.info(json.dumps(log_payload))
    print(packet.message)
    send_acknowledgement(sock, packet.seq_ack_num, client_address)


def run_server(sock: socket.socket):
    console_logger.info("Server is running")
    expected_seq_ack_num = None
    while True:
        try:
            data, client_address = sock.recvfrom(BUFFER_SIZE)
            
            packet = ReliableUPD.from_bytes(data)
            if not expected_seq_ack_num:
                expected_seq_ack_num = packet.seq_ack_num
                
            if packet.seq_ack_num == expected_seq_ack_num:
                expected_seq_ack_num += 1
                handle_client_request(sock, packet, client_address)
            else:
                console_logger.warning(f"Out of order packet: {packet.seq_ack_num}")
                log_payload = {
                    'origin': 'SERVER',
                    'timestamp': time.time(),
                    'action': "OOO_PACKET_RECEIVED",
                    'message': packet.message,
                    'seq-ack': packet.seq_ack_num
                }
                remote_logger.info(json.dumps(log_payload))
                send_acknowledgement(sock, packet.seq_ack_num, client_address)

        except socket.error:
            console_logger.info("Socket closed. Server shutting down.")
            break
        except Exception as e:
            console_logger.error(f"An error occurred in run_server: {e}")
            break


def setup_signal_handler():
    signal.signal(signal.SIGINT, handle_sigint)


def handle_sigint(sig, frame):
    # Handle Ctrl+C
    print("\nCTRL+C detected. Executing cleanup")
    cleanup()


def cleanup():
    global sockfd
    if sockfd:
        try:
            sockfd.close()
            console_logger.info("Socket closed successfully")
        except socket.error as e:
            console_logger.error(f"Error closing socket: {e}")


def main():
    global sockfd
    
    ip_addr, port = parse_arguments()
    setup_signal_handler()
    
    sockfd = create_socket(ip_addr)
    bind_socket(sockfd, ip_addr, port)

    log_payload = {
        'origin': 'SERVER',
        'timestamp': time.time(),
        'action': "START",
    }
    remote_logger.info(json.dumps(log_payload))

    run_server(sockfd)
    

if __name__ == "__main__":
    main()