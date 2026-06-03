import time
import queue
from datetime import datetime
from core.states import CoordState
from core.messages import Message
from core.logger import WALogger

SPEED = 1.2 

def sys_log(tag, node, msg):
    t = datetime.now().strftime("%H:%M:%S.%f")[:-3]
    print(f"[{t}] [{tag:<5}] [{node:<13}] {msg}")

class Coordinator:
    def __init__(self, network_queues: dict, participants: list):
        self.node_id = "COORDINATOR"
        self.network_queues = network_queues
        self.my_queue = network_queues[self.node_id]
        self.participants = participants
        self.logger = WALogger(self.node_id)
        self.state = CoordState.INITIAL
        self.votes = {}
        self.acks = {}

    def send_msg(self, receiver: str, msg_type: str, payload=None):
        msg = Message(sender=self.node_id, receiver=receiver, msg_type=msg_type, payload=payload)
        self.network_queues[receiver].put(msg)
        # 📤 LOG MẠNG: Xác nhận tin nhắn đã rời đi
        sys_log("NET  ", self.node_id, f"📤 Sent {msg_type} {f'[{payload}]' if payload else ''} -> {receiver}")
        
    def broadcast(self, msg_type: str):
        time.sleep(1.5 * SPEED)
        for p in self.participants:
            self.send_msg(p, msg_type)

    
    def change_state(self, new_state, details: str = ""):
        # 1. WRITE-AHEAD LOGGING TRƯỚC (WAL)
        self.logger.force_write(new_state.value, details)
        sys_log("WAL  ", "COORDINATOR", f"💾 Flushed to disk [COORDINATOR_log.json] -> {new_state.value}")
        
        # 2. CẬP NHẬT RAM & IN LOG SAU (STATE)
        sys_log("STATE", "COORDINATOR", f"⚙️ Transitioning to {new_state.value} ({details})")
        self.state = new_state
    
    def run(self):
        sys_log("INFO ", self.node_id, "🚀 Process initialized. Starting transaction...")
        time.sleep(2.0 * SPEED)
        
        print("\n" + "="*85)
        sys_log("PHASE", self.node_id, "🚩 BẮT ĐẦU PHA 1: BROADCASTING PREPARE (Hỏi ý kiến)")
        print("="*85)
        self.change_state(CoordState.WAIT, "Waiting for votes")
        
        # Bắn lệnh PREPARE kèm theo ID cụ thể cho từng node
        targets = {
            "Hotel": "ROOM-STD-01", 
            "Flight": "FL-VN202-ECO", 
            "Car": "CAR-ECO-01"
        }
        time.sleep(1.5 * SPEED)
        for p in self.participants:
            self.send_msg(p, "PREPARE", payload=targets[p])
        
        try:
            deadline = time.time() + (4.0 * SPEED)
            while len(self.votes) < len(self.participants):
                try:
                    msg = self.my_queue.get(timeout=1.0)
                    if msg.msg_type in ["VOTE_COMMIT", "VOTE_ABORT"]:
                        self.votes[msg.sender] = msg.msg_type
                        time.sleep(0.5 * SPEED)
                        sys_log("NET  ", self.node_id, f"📥 Received {msg.msg_type} from {msg.sender}")
                except queue.Empty:
                    remaining = int(deadline - time.time())
                    if remaining <= 0: raise queue.Empty
                    sys_log("WARN ", self.node_id, f"Waiting for votes... (Timeout in {remaining}s)")
                    
        except queue.Empty:
            print("")
            sys_log("ERROR", self.node_id, "⏰ TIMEOUT in PHASE 1. Initiating GLOBAL_ABORT.")
            self.change_state(CoordState.ABORT, "Aborting due to participant timeout")
            self.broadcast("GLOBAL_ABORT")
            return

        if "VOTE_ABORT" in self.votes.values():
            print("")
            sys_log("ERROR", self.node_id, "🛑 VOTE_ABORT detected. Initiating GLOBAL_ABORT.")
            self.change_state(CoordState.ABORT, "Global abort due to negative vote")
            self.broadcast("GLOBAL_ABORT")
            return
            
        print("\n" + "="*85)
        sys_log("PHASE", self.node_id, "🚩 BẮT ĐẦU PHA 2: PRE-COMMIT (100% YES -> Đưa vào vùng đệm)")
        print("="*85)
        self.change_state(CoordState.PRECOMMIT, "Entering PRECOMMIT buffer state")
        self.broadcast("PREPARE_TO_COMMIT")
        
        try:
            deadline = time.time() + (8.0 * SPEED)
            while len(self.acks) < len(self.participants):
                try:
                    msg = self.my_queue.get(timeout=1.0)
                    if msg.msg_type == "ACK":
                        self.acks[msg.sender] = True
                        time.sleep(0.5 * SPEED)
                        sys_log("NET  ", self.node_id, f"📥 Received ACK from {msg.sender}")
                except queue.Empty:
                    remaining = int(deadline - time.time())
                    if remaining <= 0: raise queue.Empty
                    sys_log("WARN ", self.node_id, f"Waiting for ACKs... (Timeout in {remaining}s)")
        except queue.Empty:
            pass 
            
        print("\n" + "="*85)
        sys_log("PHASE", self.node_id, "🚩 BẮT ĐẦU PHA 3: GLOBAL COMMIT (Chốt đơn)")
        print("="*85)
        self.change_state(CoordState.COMMIT, "Executing final COMMIT")
        self.broadcast("GLOBAL_COMMIT")