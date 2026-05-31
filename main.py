import multiprocessing
import time
import os
import glob
import sys
from core.participant import Participant
from core.coordinator import Coordinator

def run_participant(node_id, network_queues):
    p = Participant(node_id, network_queues)
    p.run()

def run_coordinator(network_queues, participants):
    c = Coordinator(network_queues, participants)
    c.run()

def simulate_3pc(scenario_id):
    # Dọn dẹp log trước khi chạy (Clear WAL logs)
    for f in glob.glob("logs/*.json"):
        try: os.remove(f)
        except OSError: pass

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

    # Sử dụng Manager.Queue() để giả lập IPC (Inter-Process Communication)
    manager = multiprocessing.Manager()
    network_queues = {
        "COORDINATOR": manager.Queue(),
        "Hotel": manager.Queue(),
        "Flight": manager.Queue(),
        "Car": manager.Queue()
    }
    
    participants = ["Hotel", "Flight", "Car"]
    processes = []
    
    # 1. Khởi động các tiến trình Participant (Local Recovery Managers)
    for p_id in participants:
        proc = multiprocessing.Process(target=run_participant, args=(p_id, network_queues))
        processes.append(proc)
        proc.start()
        
    # 2. Khởi động tiến trình Coordinator (Transaction Manager)
    coord_proc = multiprocessing.Process(target=run_coordinator, args=(network_queues, participants))
    coord_proc.start()

    # ==========================================
    # KỊCH BẢN ĐẠO DIỄN SỰ CỐ (FAULT INJECTION)
    # ==========================================
    
    if scenario_id == 2:
        # Giết Node Car ngay ở giây thứ 5 (Pha 1)
        time.sleep(5.0)
        print("\n" + "⚡"*20 + " [INJECTING FAULT] " + "⚡"*20)
        print("🚨 NODE 'CAR' CRASHED! COORDINATOR WILL NOT RECEIVE ALL VOTES!")
        print("⚡"*59 + "\n")
        processes[2].terminate() 
        processes[2].join()
        coord_proc.join()

    elif scenario_id == 3:
        # Rút điện Coordinator & Hotel trước khi đưa ai vào PRECOMMIT
        time.sleep(10.5) 
        print("\n\n" + "🔥"*45)
        print("🚨🚨🚨 [SYSTEM ALERT] SEVERE NETWORK PARTITION DETECTED!!!")
        print("🚨🚨🚨 COORDINATOR & HOTEL ARE DISCONNECTED FROM THE NETWORK!")
        print("🔥"*45 + "\n\n")
        coord_proc.terminate()
        processes[0].terminate()
        
    elif scenario_id == 4:
        # Rút điện Coordinator & Hotel khi Flight đã lọt vào PRECOMMIT
        time.sleep(13.5) 
        print("\n\n" + "🔥"*45)
        print("🚨🚨🚨 [SYSTEM ALERT] SEVERE NETWORK PARTITION DETECTED!!!")
        print("🚨🚨🚨 COORDINATOR & HOTEL ARE DISCONNECTED FROM THE NETWORK!")
        print("🔥"*45 + "\n\n")
        coord_proc.terminate()
        processes[0].terminate()

    else:
        # Không có sự cố, chờ Coordinator tự kết thúc
        coord_proc.join()

    # Chờ các nhân viên sống sót chạy xong Cooperative Termination Protocol
    for proc in processes:
        if proc.is_alive():
            proc.join()

    print("\n" + "="*80)
    print("🏁 SIMULATION COMPLETED")
    print("="*80 + "\n")


def print_menu():
    # Xóa sạch màn hình Terminal để UI gọn gàng
    os.system('cls' if os.name == 'nt' else 'clear')
    print("="*80)
    print("      3-PHASE COMMIT (3PC) PROTOCOL SIMULATOR - MULTI-RESOURCE RESERVATION")
    print("="*80)
    print(" Please select a scenario to simulate:\n")
    print("  [1] Happy Path (No failures, successful Global Commit)")
    print("  [2] Participant Crash (Timeout in Phase 1 -> Global Abort)")
    print("  [3] Network Partition - Early Crash (All Ready -> Global Abort)")
    print("  [4] Network Partition - Late Crash (Pre-Commit found -> Global Commit)")
    print("  [0] Exit")
    print("="*80)


if __name__ == "__main__":
    while True:
        print_menu()
        choice = input("👉 Enter your choice (0-4): ")
        
        if choice == '0':
            print("👋 Exiting simulator. Good luck with your defense!")
            sys.exit(0)
        elif choice in ['1', '2', '3', '4']:
            scenario = int(choice)
            simulate_3pc(scenario)
            input("\nPress Enter to return to the main menu...")
        else:
            print("❌ Invalid choice, please try again!")
            time.sleep(1)