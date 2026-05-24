import socket
import sys
import argparse
import threading
import signal
import time
import json
from reliable_udp import ReliableUPD
from logger import setup_logging

BUFFER_SIZE = 512

console_logger, remote_logger = None, None
sock = None  # Global socket for signal handler
    
def parse_args():
    global console_logger, remote_logger
    parser = argparse.ArgumentParser(
        description="UDP client to send messages from stdin to a server."
    )
    parser.add_argument('--target-ip',
                        required=True,
                        help="IP address of the server")
    parser.add_argument('--target-port',
                        type=int,
                        required=True,
                        help="Port number of the server")
    parser.add_argument('--timeout',
                        type=int,
                        required=True,
                        help="Timeout (in seconds) for waiting for acknowledgments")
    parser.add_argument('--max-retries',
                        type=int,
                        required=True,
                        help="Maximum number of retries per message")
    parser.add_argument('--remote-log-ip',
                        help="IP address of remote log collector")
    parser.add_argument('--remote-log-port',
                        type=int,
                        help="Port of remote log collector")
    
    args = parser.parse_args()

    try:
        console_logger, remote_logger = setup_logging(args.remote_log_ip, args.remote_log_port)
    except Exception as e:
        print(f"Error setting up logging: {e}")
        sys.exit(1)

    # Port validation
    if not (0 < args.target_port < 65536):
        console_logger.error(f"Invalid port: {args.target_port}. Must be 1-65535.")
        sys.exit(1)
        
    # Timeout and retries validation
    if args.timeout <= 0:
        console_logger.error(f"Invalid timeout: {args.timeout}. Must be > 0.")
        sys.exit(1)
        
    if args.max_retries < 0:
        console_logger.error(f"Invalid max-retries: {args.max_retries}. Must be >= 0.")
        sys.exit(1)

    return args.target_ip, args.target_port, args.timeout, args.max_retries


def create_socket(ip_addr):
    # Determine IPv4 or IPv6
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


def connect_socket(sock: socket.socket, ip_addr, port):
    try:
        sock.connect((ip_addr, port))
    except socket.error as e:
        console_logger.error(f"Connect socket error: {e}")
        sock.close()
        sys.exit(1)
    console_logger.info("Connected to server")
    return sock


def run_receiver(sock: socket.socket, pending_messages, lock):
    while True:
        try:
            data = sock.recv(BUFFER_SIZE)
            if not data:
                console_logger.info("Server closed connection, receiver thread exiting.")
                break

            packet = ReliableUPD.from_bytes(data)
            console_logger.debug(f"ACK Received: '{packet.seq_ack_num}'")

            with lock:
                flag = pending_messages.get(packet.seq_ack_num)
            
            if flag:
                flag.set()
                log_payload = {
                    'origin': 'CLIENT',
                    'timestamp': time.time(),
                    'action': "ACK_RECEIVED",
                    'message': packet.message,
                    'seq-ack': packet.seq_ack_num
                }
                remote_logger.info(json.dumps(log_payload))

            else:
                console_logger.warning(f"Received a late/unexpected ACK: '{packet.seq_ack_num}'")
                log_payload = {
                    'origin': 'CLIENT',
                    'timestamp': time.time(),
                    'action': "LATE_ACK_RECEIVED",
                    'message': packet.message,
                    'seq-ack': packet.seq_ack_num
                }
                remote_logger.info(json.dumps(log_payload))


        except socket.error:
            console_logger.info("Socket closed, receiver thread exiting.")
            break
        except Exception as e:
            console_logger.error(f"Error in receiver thread: {e}")
            break


def handle_message(sock: socket.socket, packet: ReliableUPD, timeout, max_retries, pending_messages, lock):
    flag = threading.Event()
    
    with lock:
        pending_messages[packet.seq_ack_num] = flag

    retries = 0
    ack_received = False
    
    while retries <= max_retries and not ack_received:
        try:
            console_logger.debug(f"Sending (Attempt {retries + 1}/{max_retries + 1}): '{packet.seq_ack_num}'")
            sock.send(packet.to_bytes())
            if retries == 0:
                log_payload = {
                    'origin': 'CLIENT',
                    'timestamp': time.time(),
                    'action': "PACKET_SENT",
                    'message': packet.message,
                    'seq-ack': packet.seq_ack_num
                }
            else:
                log_payload = {
                    'origin': 'CLIENT',
                    'timestamp': time.time(),
                    'action': "PACKET_RETRANSMITTED",
                    'message': packet.message,
                    'seq-ack': packet.seq_ack_num
                }
            remote_logger.info(json.dumps(log_payload))

        except socket.error as e:
            if sock:
                console_logger.error(f"Error sending message: {e}")
            break

        ack_received = flag.wait(timeout)

        if ack_received:
            console_logger.debug(f"ACK confirmed for: '{packet.seq_ack_num}'")
            break
        else:
            console_logger.warning(f"Timeout waiting for ACK (Attempt {retries + 1}) for: '{packet.seq_ack_num}'")
            retries += 1

    if not ack_received:
        console_logger.error(f"Failed to send message after {max_retries + 1} attempts: '{packet.seq_ack_num}'")

    with lock:
        if packet.seq_ack_num in pending_messages:
            del pending_messages[packet.seq_ack_num]


def run_sender(sock: socket.socket, timeout, max_retries, pending_messages, lock):
    console_logger.info("Type a message and press Enter to send. Press Ctrl+C to exit.")
    seq_ack_num = 0

    while True:
        try:
            message = input("> ")
            
            if not message:
                continue

            seq_ack_num += 1
            packet = ReliableUPD(message, seq_ack_num)

            handle_message(sock, packet, timeout, max_retries, pending_messages, lock)
        except EOFError:
            console_logger.info("\nEOF Detected. Exiting.")
            break
        except Exception as e:
            console_logger.error(f"Error in sender loop: {e}")
            break

def setup_signal_handler():
    signal.signal(signal.SIGINT, handle_sigint)


def handle_sigint(sig, frame):
    # Handle Ctrl+C
    print("\nCTRL+C detected. Executing cleanup")
    cleanup()
    sys.exit(1)

def cleanup():
    global sock
    if sock:
        try:
            sock.close()
            console_logger.info("Socket closed successfully")
        except socket.error as e:
            console_logger.error(f"Error closing socket: {e}")

def main():
    global sock
    setup_signal_handler()
    target_ip, target_port, timeout, max_retries = parse_args()

    # shared between threads
    pending_messages = {}
    lock = threading.Lock()

    sock = create_socket(target_ip)
    connect_socket(sock, target_ip, target_port)

    # Start the receiver thread
    receiver_thread = threading.Thread(
        target=run_receiver,
        args=(sock, pending_messages, lock),
        name="Receiver"
    )
    receiver_thread.daemon = True
    receiver_thread.start()

    log_payload = {
        'origin': 'CLIENT',
        'timestamp': time.time(),
        'action': "START",
        'timeout': timeout,
        'max_retries': max_retries,
    }
    remote_logger.info(json.dumps(log_payload))

    run_sender(sock, timeout, max_retries, pending_messages, lock)

    cleanup()


if __name__ == "__main__":
    main()