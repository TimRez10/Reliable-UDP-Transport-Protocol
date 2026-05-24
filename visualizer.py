import json
import matplotlib.pyplot as plt
import matplotlib.animation as animation
import collections

LOG_FILE = "proxy_logs.jsonl"
WINDOW_SIZE = 20 

packet_stats = collections.defaultdict(lambda: {
    "s_drop": 0, "c_drop": 0, 
    "c_send": 0, "c_ret": 0, "c_recv": 0, 
    "s_sent": 0, "c_late_recv": 0,
})

try:
    f = open(LOG_FILE, "r")
    f.seek(0, 2)
except FileNotFoundError:
    open(LOG_FILE, "w").close()
    f = open(LOG_FILE, "r")

def read_new_lines():
    while True:
        line = f.readline()
        if not line: break
        
        try:
            entry = json.loads(line)
            
            if 'seq-ack' not in entry: continue
            
            seq = int(entry['seq-ack'])
            stats = packet_stats[seq]
            
            action = entry.get('action', '')
            origin = entry.get('origin', '')
            direction = entry.get('direction', '')
            
            if origin == "PROXY" and action == "DROP_PACKET":
                if direction == "Server->Client": stats["s_drop"] += 1
                else: stats["c_drop"] += 1
            
            elif origin == "CLIENT":
                if action == "PACKET_SENT": stats["c_send"] += 1
                elif action == "PACKET_RETRANSMITTED": stats["c_ret"] += 1
                elif action == "ACK_RECEIVED": stats["c_recv"] += 1
                elif action == "LATE_ACK_RECEIVED": stats["c_late_recv"] += 1
            
            elif origin == "SERVER" and action == "ACK_SENT":
                stats["s_sent"] += 1
                
        except (json.JSONDecodeError, ValueError):
            continue

def animate(i, ax):
    read_new_lines()
    
    ax.clear()
    ax.set_title("Packet Health (Events per Sequence)")
    ax.set_xlabel("Sequence Number")
    ax.set_ylabel("Event Count")
    
    all_seqs = sorted(packet_stats.keys())
    if not all_seqs: return
    
    display_seqs = all_seqs[-WINDOW_SIZE:]
    
    s_drops = [packet_stats[s]["s_drop"] for s in display_seqs]
    c_drops = [packet_stats[s]["c_drop"] for s in display_seqs]
    c_sends = [packet_stats[s]["c_send"] for s in display_seqs]
    c_rets  = [packet_stats[s]["c_ret"]  for s in display_seqs]
    c_recvs = [packet_stats[s]["c_recv"] for s in display_seqs]
    s_sents = [packet_stats[s]["s_sent"] for s in display_seqs]
    c_late_recvs = [packet_stats[s]["c_late_recv"] for s in display_seqs]
    
    x_pos = range(len(display_seqs))
    
    # Layer 1
    ax.bar(x_pos, s_drops, label='Drop (S->C)', color='purple')
    
    # Layer 2
    ax.bar(x_pos, c_drops, bottom=s_drops, label='Drop (C->S)', color='red')
    
    # Layer 3
    bot_2 = [sum(x) for x in zip(s_drops, c_drops)]
    ax.bar(x_pos, c_sends, bottom=bot_2, label='Client Sent', color='blue')
    
    # Layer 4
    bot_3 = [sum(x) for x in zip(bot_2, c_sends)]
    ax.bar(x_pos, c_rets, bottom=bot_3, label='Retransmit', color='teal')
    
    # Layer 5
    bot_4 = [sum(x) for x in zip(bot_3, c_rets)]
    ax.bar(x_pos, s_sents, bottom=bot_4, label='Server ACK', color='green')

    # Layer 6
    bot_5 = [sum(x) for x in zip(bot_4, s_sents)]
    ax.bar(x_pos, c_recvs, bottom=bot_5, label='Client ACK Recv', color='yellow')

    # Layer 7
    bot_6 = [sum(x) for x in zip(bot_5, c_recvs)]
    ax.bar(x_pos, c_late_recvs, bottom=bot_6, label='Late ACK Recv', color='khaki')

    ax.set_xticks(x_pos)
    ax.set_xticklabels(display_seqs)
    ax.legend(loc='upper left', fontsize='small', ncol=2)
    ax.grid(axis='y', linestyle='--', alpha=0.3)

def main():
    fig, ax = plt.subplots(figsize=(10, 6))
    ani = animation.FuncAnimation(fig, animate, fargs=(ax,), interval=1000)
    try:
        plt.show()
    except KeyboardInterrupt:
        pass
    f.close()

if __name__ == "__main__":
    main()