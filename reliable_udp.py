class ReliableUPD:
    def __init__(self, message: str, syn_num: int):
        self.message = message 
        self.seq_ack_num = syn_num # client uses it for sequence, server uses it for ack
    
    def to_hex(self):
        message_bytes = self.message.encode('utf-8')
        message_hex = message_bytes.hex()
        return f"{self.seq_ack_num:08X}{message_hex}"

    def to_bytes(self):
        hex_string = self.to_hex()
        return bytes.fromhex(hex_string)

    @staticmethod
    def from_bytes(payload):
        hex = payload.hex().upper()
        
        seq_ack_num = int(hex[0:8], 16)
        message_hex = hex[8:]

        try:
            message_bytes = bytes.fromhex(message_hex)
        except ValueError:
            message_bytes = b''
        message_str = message_bytes.decode('utf-8')

        return ReliableUPD(message_str, seq_ack_num)