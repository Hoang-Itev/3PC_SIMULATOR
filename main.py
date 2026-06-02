import multiprocessing
import time
import os
import glob
import sys
import json
from core.participant import Participant
from core.coordinator import Coordinator

def reset_databases():
    """Chỉ gọi khi người dùng muốn Reset kho hàng bằng tay"""
    print("\n" + "="*80)
    print("🔄 ĐANG NẠP LẠI KHO HÀNG (RESET DATABASES)...")
    for db in ["datasets/hotel_db.json", "datasets/flight_db.json", "datasets/car_db.json"]:
        with open(db, "w") as f:
            r_name = "Hotel_Rooms" if "hotel" in db else ("Flight_Seats" if "flight" in db else "Car_Rentals")
            json.dump({"resource": r_name, "available": 10, "locked": 0}, f, indent=4)
            print(f"  ➔ Đã khôi phục {db} (Kho: 10, Khóa: 0)")
    print("="*80 + "\n")

def clear_logs():
    """Dọn dẹp log cũ của lần chạy trước"""
    for f in glob.glob("logs/*.json"):
        try: os.remove(f)
        except OSError: pass

def run_participant(node_id, network_queues):
    p = Participant(node_id, network_queues)
    p.run()

def run_coordinator(network_queues, participants):
    c = Coordinator(network_queues, participants)
    c.run()

def manual_recovery_demo():
    """Hàm độc lập để demo cắm điện hồi sinh Coordinator và Hotel (Tính năng số 5)"""
    print("\n" + "="*80)
    print("🔌 RECOVERY PROTOCOL INITIATED (Khôi phục máy chủ bị sập)...")
    print("="*80)
    time.sleep(1)
    
    # 1. Khôi phục Hotel
    hotel_state = "INITIAL"
    if os.path.exists("logs/Hotel_log.json"):
        with open("logs/Hotel_log.json") as f:
            logs = json.load(f)
            if logs: hotel_state = logs[-1]["state"]
    print(f"[HOTEL RECOVERY] Đọc ổ cứng: Kẹt ở [{hotel_state}] trước khi mất điện.")
    time.sleep(1.5)

    if hotel_state in ["READY", "PRECOMMIT"]:
        print("[HOTEL RECOVERY] Đang gọi mạng P2P hỏi thăm Flight: 'Mọi người xử lý sao rồi?'")
        time.sleep(1.5)
        
        flight_state = "UNKNOWN"
        if os.path.exists("logs/Flight_log.json"):
            with open("logs/Flight_log.json") as f:
                f_logs = json.load(f)
                if f_logs: flight_state = f_logs[-1]["state"]
                
        print(f"➔ Flight trả lời: 'Sếp chết rồi, tụi tao tự chốt {flight_state} toàn cục!'")
        time.sleep(1.5)
        
        print(f"[HOTEL RECOVERY] Đồng bộ dữ liệu: Chuyển sang {flight_state} và CẬP NHẬT KHO PHÒNG.")
        db_path = "datasets/hotel_db.json"
        if os.path.exists(db_path):
            with open(db_path, "r+") as f:
                db = json.load(f)
                if db["locked"] > 0:
                    if flight_state == "ABORT":
                        db["available"] += db["locked"] # Hoàn trả phòng
                    db["locked"] = 0 # Xóa khóa
                f.seek(0)
                json.dump(db, f, indent=4)
                f.truncate()
        
    # 2. Khôi phục Coordinator
    time.sleep(1)
    print("\n[COORD RECOVERY] Máy chủ Sếp có điện lại. Hỏi thăm Flight...")
    time.sleep(1.5)
    print(f"➔ Sếp ghi nhận giao dịch đã được anh em chốt {flight_state}. Cập nhật sổ tay!")
    time.sleep(1)
    
    print("\n✅ HỆ THỐNG ĐÃ PHỤC HỒI TOÀN VẸN (Consistency Achieved)!\n")

def simulate_3pc(scenario_id):
    # ĐÃ GỠ BỎ TỰ ĐỘNG RESET DB. Giữ nguyên hiện trạng Database.
    clear_logs()
        
    titles = {
        1: "✅ SCENARIO 1: HAPPY PATH (SUCCESSFUL COMMIT)",
        2: "⚠️ SCENARIO 2: PARTICIPANT CRASH IN PHASE 1 (COORDINATOR TIMEOUT -> ABORT)",
        3: "🚨 SCENARIO 3: NETWORK PARTITION - EARLY CRASH (TERMINATION -> ABORT)",
        4: "🚨 SCENARIO 4: NETWORK PARTITION - LATE CRASH (TERMINATION -> COMMIT)"
    }

    print("\n" + "="*80)
    print("STARTING 3-PHASE COMMIT (3PC) MULTIPROCESSING SIMULATOR")
    print(titles.get(scenario_id, "UNKNOWN SCENARIO"))
    print("="*80 + "\n")

    manager = multiprocessing.Manager()
    network_queues = {
        "COORDINATOR": manager.Queue(),
        "Hotel": manager.Queue(),
        "Flight": manager.Queue(),
        "Car": manager.Queue()
    }
    
    participants = ["Hotel", "Flight", "Car"]
    processes = []
    
    for p_id in participants:
        proc = multiprocessing.Process(target=run_participant, args=(p_id, network_queues))
        processes.append(proc)
        proc.start()
        
    coord_proc = multiprocessing.Process(target=run_coordinator, args=(network_queues, participants))
    coord_proc.start()

   # KỊCH BẢN ĐẠO DIỄN SỰ CỐ
    if scenario_id == 2:
        time.sleep(5.0)
        print("\n" + "⚡"*20 + " [INJECTING FAULT] " + "⚡"*20)
        print("🚨 NODE 'CAR' CRASHED! COORDINATOR WILL NOT RECEIVE ALL VOTES!")
        print("⚡"*59 + "\n")
        processes[2].terminate() 
        processes[2].join()
        coord_proc.join()

    elif scenario_id == 3:
        # SỬA Ở ĐÂY: Cho chết ở giây thứ 7.5 (Đảm bảo đang ở Pha 1, chưa ai kịp vào PRECOMMIT)
        time.sleep(7.5) 
        print("\n\n" + "🔥"*45)
        print("🚨🚨🚨 [SYSTEM ALERT] SEVERE NETWORK PARTITION DETECTED!!!")
        print("🚨🚨🚨 COORDINATOR & HOTEL ARE DISCONNECTED FROM THE NETWORK!")
        print("🔥"*45 + "\n\n")
        coord_proc.terminate()
        processes[0].terminate()
        
    elif scenario_id == 4:
        # SỬA Ở ĐÂY: Cho chết ở giây thứ 11.0 (Đảm bảo vừa lọt vào Pha 2 PRECOMMIT)
        time.sleep(11.0) 
        print("\n\n" + "🔥"*45)
        print("🚨🚨🚨 [SYSTEM ALERT] SEVERE NETWORK PARTITION DETECTED!!!")
        print("🚨🚨🚨 COORDINATOR & HOTEL ARE DISCONNECTED FROM THE NETWORK!")
        print("🔥"*45 + "\n\n")
        coord_proc.terminate()
        processes[0].terminate()

    else:
        coord_proc.join()

    # Chờ Flight và Car sống sót chạy xong việc xả DB của riêng tụi nó
    for proc in processes:
        if proc.is_alive():
            proc.join()

    print("\n" + "="*80)
    print("🏁 SIMULATION COMPLETED")
    print("="*80 + "\n")


def print_menu():
    os.system('cls' if os.name == 'nt' else 'clear')
    print("="*80)
    print("      3-PHASE COMMIT (3PC) PROTOCOL SIMULATOR - MULTI-RESOURCE RESERVATION")
    print("="*80)
    print(" Please select a scenario to simulate:\n")
    print("  [1] Happy Path (No failures, successful Global Commit)")
    print("  [2] Participant Crash (Timeout in Phase 1 -> Global Abort)")
    print("  [3] Network Partition - Early Crash (All Ready -> Global Abort)")
    print("  [4] Network Partition - Late Crash (Pre-Commit found -> Global Commit)")
    print("  [5] 🔌 Manual Recovery (Reboot crashed Hotel & Coordinator)")
    print("  [6] ♻️ Reset Databases (Refill inventory to 10)")
    print("  [0] Exit")
    print("="*80)


if __name__ == "__main__":
    while True:
        print_menu()
        choice = input("👉 Enter your choice (0-6): ")
        
        if choice == '0':
            print("👋 Exiting simulator. Good luck with your defense!")
            sys.exit(0)
        elif choice in ['1', '2', '3', '4']:
            scenario = int(choice)
            simulate_3pc(scenario)
            input("\nPress Enter to return to the main menu...")
        elif choice == '5':
            manual_recovery_demo()
            input("\nPress Enter to return to the main menu...")
        elif choice == '6':
            reset_databases()
            input("\nPress Enter to return to the main menu...")
        else:
            print("❌ Invalid choice, please try again!")
            time.sleep(1)