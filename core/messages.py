from dataclasses import dataclass
from typing import Any, Optional

@dataclass
class Message:
    """
    Gói tin dùng để truyền qua lại giữa các tiến trình (OS Processes)
    bằng multiprocessing.Queue.
    """
    sender: str
    receiver: str
    msg_type: str         # Các loại lệnh: PREPARE, VOTE_COMMIT, PREPARE_TO_COMMIT, GLOBAL_COMMIT...
    payload: Optional[Any] = None