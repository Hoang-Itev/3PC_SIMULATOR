import json
import os

class WALogger:
    """
    Bộ ghi Write-Ahead Logging (WAL) đảm bảo tính Durability.
    Tham chiếu: Özsu, Section 5.1 - WAL protocol.
    """
    def __init__(self, node_id: str):
        self.log_file = f"logs/{node_id}_log.json"
        if not os.path.exists("logs"):
            os.makedirs("logs")
            
        # Khởi tạo log trống nếu chưa có
        with open(self.log_file, 'w') as f:
            json.dump([], f)

    def force_write(self, state: str, details: str = ""):
        """
        BẮT BUỘC phải gọi hàm này ghi xuống đĩa TRƯỚC KHI gửi tin nhắn đi.
        """
        with open(self.log_file, 'r+') as f:
            logs = json.load(f)
            logs.append({"state": state, "details": details})
            f.seek(0)
            json.dump(logs, f, indent=4)
            f.truncate()
            f.flush() # Ép hệ điều hành ghi vật lý xuống ổ cứng ngay lập tức
            os.fsync(f.fileno())

    def read_last_state(self) -> str:
        """Dành cho Recovery Protocol khi máy chủ có điện lại"""
        if not os.path.exists(self.log_file):
            return "INITIAL"
        with open(self.log_file, 'r') as f:
            logs = json.load(f)
            if not logs:
                return "INITIAL"
            return logs[-1]["state"]