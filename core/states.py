from enum import Enum

class CoordState(Enum):
    INITIAL = "INITIAL"
    WAIT = "WAIT"             # Đã gửi Prepare, chờ vote
    PRECOMMIT = "PRECOMMIT"   # (3PC) Đã gom đủ 100% Yes, chờ Ack để chốt
    COMMIT = "COMMIT"
    ABORT = "ABORT"

class PartState(Enum):
    INITIAL = "INITIAL"
    READY = "READY"           # Đã khóa tài nguyên và Vote YES (Làm con tin)
    PRECOMMIT = "PRECOMMIT"   # (3PC) Vùng đệm an toàn, biết chắc mọi người đã Vote YES
    COMMIT = "COMMIT"
    ABORT = "ABORT"