import logging
import logging.handlers
import socket
import sys

def setup_logging(log_collector_ip, log_collector_port):
    console_logger = logging.getLogger('console')
    console_logger.setLevel(logging.WARNING)
    c_handler = logging.StreamHandler(sys.stdout)
    c_handler.setFormatter(logging.Formatter('%(levelname)s - %(message)s'))
    console_logger.addHandler(c_handler)

    remote_logger = logging.getLogger('remote')
    remote_logger.setLevel(logging.INFO)

    if not log_collector_ip and not log_collector_port: # if not provided
        console_logger.warning("IP and/or port not provided for remote logger. Skipping...")
        return console_logger, remote_logger

    try:
        r_handler = logging.handlers.SysLogHandler(
            address=(log_collector_ip, log_collector_port), 
            socktype=socket.SOCK_STREAM
        )
        r_handler.setFormatter(logging.Formatter('%(message)s'))
        r_handler.append_nul = False 
        
        remote_logger.addHandler(r_handler)
    except socket.error:
        console_logger.error("Could not connect to remote log collector")
    
    return console_logger, remote_logger