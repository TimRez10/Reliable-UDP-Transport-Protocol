import socket
import sys
import json
import time
import random
import argparse
import threading
import signal
import queue
from logger import setup_logging
from reliable_udp import ReliableUPD

BUFFER_SIZE = 512

console_logger, remote_logger = None, None
sock = None  # Global socket for signal handler

# to share client address across threads
class ClientAddress:
    def __init__(self):
        self.client_address = None
        self.lock = threading.Lock()

    def update_client(self, addr):
        with self.lock:
            if self.client_address != addr:
                self.client_address = addr
                console_logger.info(f"New client detected: {self.client_address}")

    def get_client(self):
        with self.lock:
            return self.client_address


def parse_arguments():
    global console_logger, remote_logger
    parser = argparse.ArgumentParser(
        description="A UDP proxy that sits between a client and server to "
                    "simulate unreliable network conditions (delay and drop) "
                    "independently in both directions."
    )
    
    parser.add_argument('--listen-ip',
                        required=True,
                        help="IP address to bind for client packets")
    parser.add_argument('--listen-port',
                        type=int,
                        required=True,
                        help="Port to listen on for client packets")
    parser.add_argument('--target-ip',
                        required=True,
                        help="Server IP address to forward packets to")
    parser.add_argument('--target-port',
                        type=int,
                        required=True,
                        help="Server port number")
    parser.add_argument('--client-drop',
                        type=int,
                        default=0,
                        help="Drop chance (%%) for packets from client (default: 0)")
    parser.add_argument('--client-delay',
                        type=int,
                        default=0,
                        help="Delay chance (%%) for packets from client (default: 0)")
    parser.add_argument('--client-delay-time-min',
                        type=int,
                        default=0,
                        help="Minimum delay time (ms) for client packets (default: 0)")
    parser.add_argument('--client-delay-time-max',
                        type=int,
                        default=0,
                        help="Maximum delay time (ms) for client packets (default: 0)")
    parser.add_argument('--server-drop',
                        type=int,
                        default=0,
                        help="Drop chance (%%) for packets from server (default: 0)")
    parser.add_argument('--server-delay',
                        type=int,
                        default=0,
                        help="Delay chance (%%) for packets from server (default: 0)")
    parser.add_argument('--server-delay-time-min',
                        type=int,
                        default=0,
                        help="Minimum delay time (ms) for server packets (default: 0)")
    parser.add_argument('--server-delay-time-max',
                        type=int,
                        default=0,
                        help="Maximum delay time (ms) for server packets (default: 0)")
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
    if not (0 < args.listen_port < 65536):
        console_logger.error(f"Invalid listen-port: {args.listen_port}. Must be 1-65535.")
        sys.exit(1)
    if not (0 < args.target_port < 65536):
        console_logger.error(f"Invalid target-port: {args.target_port}. Must be 1-65535.")
        sys.exit(1)

    # Percentage validation
    if not (0 <= args.client_drop <= 100):
        console_logger.error(f"Invalid client-drop: {args.client_drop}%%. Must be 0-100.")
        sys.exit(1)
    if not (0 <= args.server_drop <= 100):
        console_logger.error(f"Invalid server-drop: {args.server_drop}%%. Must be 0-100.")
        sys.exit(1)
    if not (0 <= args.client_delay <= 100):
        console_logger.error(f"Invalid client-delay: {args.client_delay}%%. Must be 0-100.")
        sys.exit(1)
    if not (0 <= args.server_delay <= 100):
        console_logger.error(f"Invalid server-delay: {args.server_delay}%%. Must be 0-100.")
        sys.exit(1)
        
    # Delay time validation
    if args.client_delay_time_min < 0:
        console_logger.error(f"Invalid client-delay-time-min: {args.client_delay_time_min}. "
                      "Cannot be negative.")
        sys.exit(1)
    if args.client_delay_time_max < args.client_delay_time_min:
        console_logger.error(f"Invalid client delay range: max ({args.client_delay_time_max}) "
                      f"is less than min ({args.client_delay_time_min}).")
        sys.exit(1)
    if args.server_delay_time_min < 0:
        console_logger.error(f"Invalid server-delay-time-min: {args.server_delay_time_min}. "
                      "Cannot be negative.")
        sys.exit(1)
    if args.server_delay_time_max < args.server_delay_time_min:
        console_logger.error(f"Invalid server delay range: max ({args.server_delay_time_max}) "
                      f"is less than min ({args.server_delay_time_min}).")
        sys.exit(1)

    return args


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


def bind_socket(sock: socket.socket, ip_addr, port):
    try:
        sock.bind((ip_addr, port))
        console_logger.debug(f"Bound to socket: {ip_addr}:{port}")
    except socket.error as e:
        console_logger.error(f"Binding failed: {e}")
        sys.exit(1)


def delay_or_drop_packet(packet, drop_chance, delay_chance, delay_min, delay_max, direction_label):
    if drop_chance > 0:
        if random.randint(1, 100) <= drop_chance:
            if packet.message:
                console_logger.warning(f"[{direction_label}] Packet with message '{packet.message}' DROPPED")
            else:
                console_logger.warning(f"[{direction_label}] ACK packet DROPPED")
            log_payload = {
                'origin': 'PROXY',
                'timestamp': time.time(),
                'action': "DROP_PACKET",
                'direction': direction_label,
                'message': packet.message,
                'seq-ack': packet.seq_ack_num
            }
            remote_logger.info(json.dumps(log_payload))

            return False

    if delay_chance > 0:
        if random.randint(1, 100) <= delay_chance:
            delay_ms = random.randint(delay_min, delay_max)
            delay_sec = delay_ms / 1000.0
            if packet.message:
                console_logger.warning(f"[{direction_label}] Delaying packet with message '{packet.message}' for {delay_ms}ms...")
            else:
                console_logger.warning(f"[{direction_label}] Delaying ACK packet for {delay_ms}ms...")
            log_payload = {
                'origin': 'PROXY',
                'timestamp': time.time(),
                'action': "DELAY_PACKET",
                'direction': direction_label,
                'delay_length': delay_ms,
                'message': packet.message,
                'seq-ack': packet.seq_ack_num
            }
            remote_logger.info(json.dumps(log_payload))

            time.sleep(delay_sec)
    
    return True


def client_to_server(sock, args, client_state, client_to_server_queue: queue.Queue):
    target_addr = (args.target_ip, args.target_port)
    while True:
        data, source_addr = client_to_server_queue.get()
        client_state.update_client(source_addr)

        packet = ReliableUPD.from_bytes(data)

        should_send = delay_or_drop_packet(
            packet,
            args.client_drop,
            args.client_delay,
            args.client_delay_time_min,
            args.client_delay_time_max,
            "Client->Server"
        )
                
        if should_send:
            handle_client_to_server(sock, data, target_addr)
        client_to_server_queue.task_done()


def handle_client_to_server(sock, data, target_addr):
    try:
        sock.sendto(data, target_addr)
        console_logger.debug(f"Client -> Server ({len(data)} bytes)")
        packet = ReliableUPD.from_bytes(data)
        log_payload = {
            'origin': 'PROXY',
            'timestamp': time.time(),
            'action': "PACKET_SENT",
            'direction': "Client->Server",
            'message': packet.message,
            'seq-ack': packet.seq_ack_num
        }
        remote_logger.info(json.dumps(log_payload))
    except Exception as e:
        console_logger.error(f"Send Error Client -> Server: {e}")


def server_to_client(sock, args, client_state, server_to_client_queue: queue.Queue):
    while True:
        data, source_addr = server_to_client_queue.get()
        client_address = client_state.get_client()
        if client_address is None:
            console_logger.warning("Dropped server packet: No client known yet")
            server_to_client_queue.task_done()
            continue

        packet = ReliableUPD.from_bytes(data)

        should_send = delay_or_drop_packet(
            packet,
            args.server_drop,
            args.server_delay,
            args.server_delay_time_min,
            args.server_delay_time_max,
            "Server->Client"
        )

        if should_send:
            handle_server_to_client(sock, data, client_address)
        server_to_client_queue.task_done()


def handle_server_to_client(sock, data, client_addr):
    try:
        sock.sendto(data, client_addr)
        console_logger.debug(f"Server -> Client ({len(data)} bytes)")
        packet = ReliableUPD.from_bytes(data)
        log_payload = {
            'origin': 'PROXY',
            'timestamp': time.time(),
            'action': "PACKET_SENT",
            'direction': "Server->Client",
            'message': packet.message,
            'seq-ack': packet.seq_ack_num
        }
        remote_logger.info(json.dumps(log_payload))
    except Exception as e:
        console_logger.error(f"Send Error Server -> Client: {e}")


def run_proxy(sock, target_address, server_to_client_queue, client_to_server_queue):
    console_logger.info("Proxy running. Press Ctrl+C to stop.")
    while True:
        data, source_addr = sock.recvfrom(BUFFER_SIZE)

        if source_addr[:2] == target_address[:2]: # From server
            console_logger.debug(f"Server->Client Packet detected")
            packet = ReliableUPD.from_bytes(data)
            log_payload = {
                'origin': 'PROXY',
                'timestamp': time.time(),
                'action': "PACKET_RECEIVED",
                'direction': "Server->Client",
                'message': packet.message,
                'seq-ack': packet.seq_ack_num
            }
            remote_logger.info(json.dumps(log_payload))

            server_to_client_queue.put((data, source_addr))

        else: # From client
            console_logger.debug(f"Client->Server Packet detected")
            packet = ReliableUPD.from_bytes(data)
            log_payload = {
                'origin': 'PROXY',
                'timestamp': time.time(),
                'action': "PACKET_RECEIVED",
                'direction': "Client->Server",
                'message': packet.message,
                'seq-ack': packet.seq_ack_num
            }
            remote_logger.info(json.dumps(log_payload))

            client_to_server_queue.put((data, source_addr))


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
    args = parse_arguments()
    
    sock = create_socket(args.listen_ip)
    bind_socket(sock, args.listen_ip, args.listen_port)

    # Shared between threads
    client_state = ClientAddress()
    server_to_client_queue = queue.Queue()
    client_to_server_queue = queue.Queue()

    # Create seperate threads
    client_to_server_thread = threading.Thread(
        target=client_to_server,
        args=(sock, args, client_state, client_to_server_queue)
    )
    server_to_client_thread = threading.Thread(
        target=server_to_client,
        args=(sock, args, client_state, server_to_client_queue)
    )
    client_to_server_thread.daemon = True
    server_to_client_thread.daemon = True
    client_to_server_thread.start()
    server_to_client_thread.start()
    target_addr = (args.target_ip, args.target_port)

    log_payload = {
        'origin': 'PROXY',
        'timestamp': time.time(),
        'action': "START",
        'client_drop': args.client_drop,
        'server_drop': args.server_drop,
        'client_delay': args.client_delay,
        'server_delay': args.server_delay,
        'client_delay_time_min': args.client_delay_time_min,
        'client_delay_time_max': args.client_delay_time_max,
        'server_delay_time_min': args.server_delay_time_min,
        'server_delay_time_min': args.server_delay_time_min,
    }
    remote_logger.info(json.dumps(log_payload))

    run_proxy(sock, target_addr, server_to_client_queue, client_to_server_queue)


if __name__ == "__main__":
    main()