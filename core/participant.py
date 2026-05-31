import time
import queue
from datetime import datetime
from core.states import PartState
from core.messages import Message
from core.logger import WALogger

SPEED = 1.2 

def sys_log(tag, node, msg):
    t = datetime.now().strftime("%H:%M:%S.%f")[:-3]
    print(f"[{t}] [{tag:<5}] [{node:<13}] {msg}")

class Participant:
    def __init__(self, node_id: str, network_queues: dict):
        self.node_id = node_id
        self.network_queues = network_queues
        self.my_queue = network_queues[node_id]
        self.logger = WALogger(node_id)
        
        last_state = self.logger.read_last_state()
        self.state = PartState(last_state)
        
        self.peers = [pid for pid in network_queues.keys() if pid != "COORDINATOR" and pid != self.node_id]
        self.peer_states = {}
        self.is_terminating = False 

    def send_msg(self, receiver: str, msg_type: str, payload=None):
        time.sleep(0.5 * SPEED)
        msg = Message(sender=self.node_id, receiver=receiver, msg_type=msg_type, payload=payload)
        self.network_queues[receiver].put(msg)
        sys_log("NET  ", self.node_id, f"📤 Sent {msg_type} -> {receiver}")

    def change_state(self, new_state: PartState, details: str = ""):
        sys_log("STATE", self.node_id, f"⚙️ Transitioning to {new_state.value} ({details})")
        self.state = new_state
        
        self.logger.force_write(new_state.value, details)
        sys_log("WAL  ", self.node_id, f"💾 Flushed to disk [{self.node_id}_log.json] -> {new_state.value}")
        time.sleep(1.0 * SPEED)

    def run(self):
        sys_log("INFO ", self.node_id, f"🚀 Process initialized. State: {self.state.value}")
        
        while True:
            try:
                timeout_limit = 3.0 * SPEED if self.is_terminating else 12.0 * SPEED
                deadline = time.time() + timeout_limit
                
                msg = None
                while True:
                    try:
                        msg = self.my_queue.get(timeout=1.0)
                        if msg:
                            sys_log("NET  ", self.node_id, f"📥 Received {msg.msg_type} from {msg.sender}")
                        break 
                    except queue.Empty:
                        remaining = int(deadline - time.time())
                        if remaining <= 0: raise queue.Empty
                        if self.is_terminating:
                            sys_log("WARN ", self.node_id, f"Waiting for peer responses... (Timeout in {remaining}s)")
                            
                if msg:
                    self.process_message(msg)
                
            except queue.Empty:
                self.handle_timeout()
            
            if self.state in [PartState.COMMIT, PartState.ABORT]:
                time.sleep(0.5 * SPEED)
                sys_log("INFO ", self.node_id, "🏁 Process terminated safely.")
                break

    def process_message(self, msg: Message):
        if msg.msg_type == "PREPARE" and self.state == PartState.INITIAL:
            self.change_state(PartState.READY, "Resources locked, VOTE_COMMIT sent")
            self.send_msg("COORDINATOR", "VOTE_COMMIT")

        elif msg.msg_type == "PREPARE_TO_COMMIT" and self.state == PartState.READY:
            self.change_state(PartState.PRECOMMIT, "Entered safe buffer state (PRECOMMIT)")
            self.send_msg("COORDINATOR", "ACK")

        elif msg.msg_type == "GLOBAL_COMMIT":
            self.change_state(PartState.COMMIT, "Executing COMMIT command")
            
        elif msg.msg_type == "GLOBAL_ABORT":
            self.change_state(PartState.ABORT, "Executing ABORT command")

        elif msg.msg_type == "REQ_STATE":
            self.send_msg(msg.sender, "REP_STATE", payload=self.state.value)

        elif msg.msg_type == "REP_STATE":
            self.peer_states[msg.sender] = msg.payload

    def handle_timeout(self):
        if self.state == PartState.INITIAL:
            print("")
            sys_log("ERROR", self.node_id, "TIMEOUT! Lost connection to Coordinator.")
            self.change_state(PartState.ABORT, "Unilateral Abort executed")
            
        elif self.state in [PartState.READY, PartState.PRECOMMIT]:
            if not self.is_terminating:
                print("")
                sys_log("ERROR", self.node_id, "TIMEOUT! Lost connection to Coordinator.")
                time.sleep(0.5 * SPEED)
                sys_log("INFO ", self.node_id, f"Initiating Cooperative Termination. Requesting state from peers: {self.peers}")
                
                self.is_terminating = True
                self.peer_states.clear()
                for peer in self.peers:
                    self.send_msg(peer, "REQ_STATE")
            else:
                print("")
                sys_log("ERROR", self.node_id, "Peer timeout reached. Evaluating state with available nodes...")
                self.evaluate_termination_rules()

    def evaluate_termination_rules(self):
        time.sleep(1.0 * SPEED)
        states_list = list(self.peer_states.values()) + [self.state.value]
        sys_log("INFO ", self.node_id, f"🔍 Collected states from operational nodes: {states_list}")
        time.sleep(1.5 * SPEED) 

        if "COMMIT" in states_list:
            self.change_state(PartState.COMMIT, "Transitioning based on peer's COMMIT state")
            return
        if "ABORT" in states_list:
            self.change_state(PartState.ABORT, "Transitioning based on peer's ABORT state")
            return
            
        if "PRECOMMIT" in states_list:
            sys_log("INFO ", self.node_id, "💡 PRECOMMIT state detected! Coordinator must have received all YES votes.")
            self.change_state(PartState.COMMIT, "Global COMMIT decided via Termination Protocol")
            return
            
        if all(s == "READY" for s in states_list):
            sys_log("INFO ", self.node_id, "💡 All operational nodes are in READY state. Cannot determine Coordinator's decision.")
            self.change_state(PartState.ABORT, "Global ABORT decided via Termination Protocol")