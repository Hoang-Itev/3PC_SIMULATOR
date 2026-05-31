# 3-Phase Commit (3PC) Simulator: Multi-Resource Reservation

**Team:** Distributed Dynamics  
**Member:** Đinh Việt Hoàng  
**Course:** Distributed Database Systems  
**Project ID:** #34 - Reliability and Commit Protocols  

---

## 📖 Project Overview

The standard 2-Phase Commit (2PC) protocol is inherently blocking. If the Coordinator fails while participants are in the `READY` state, the entire system is paralyzed. 

This project is a Python-based simulator for the **3-Phase Commit (3PC) protocol**, designed to solve this blocking vulnerability. It models a distributed travel reservation ecosystem (Hotel, Flight, Car) coordinating with a Transaction Manager. The system strictly adheres to the theoretical state machines and **Cooperative Termination Protocol** defined in academic literature.

![3PC State Machine Diagram](assets/state_machine.png)
*(Figure 1: State transitions for the Coordinator and Participants in 3PC)*

---

## 🛠️ Tech Stack & Architecture

- **Language:** Python 3.x (Object-Oriented Design)
- **Concurrency:** `multiprocessing` library to create truly isolated processes for the Coordinator and each Participant.
- **Network Simulation:** `multiprocessing.Manager().Queue()` for Inter-Process Communication (IPC) to simulate asynchronous network routing, polling, and artificial latency.
- **Durability:** Write-Ahead Logging (WAL) utilizing local `.json` files to simulate non-volatile physical disk storage.

---

## 🚀 Installation & Usage

### 1. Prerequisites
Ensure you have Python 3.8+ installed. No external third-party libraries are required (only Python standard libraries are used).

### 2. Clone the Repository
```bash
git clone <YOUR-GITHUB-REPO-LINK-HERE>
cd 3pc-simulator
### 3. Run the Simulator
Launch the interactive terminal menu by running:
```bash
python main.py
```

![Terminal Execution](assets/terminal_demo.png)
*(Placeholder: Add a screenshot of your terminal running the code here)*

---

## 🎬 Simulation Scenarios (The Proof)

Upon running `main.py`, you will be presented with an interactive menu containing 4 rigorously tested scenarios to prove the system's fault tolerance:

* **[1] Happy Path (Successful Global Commit):** Demonstrates a flawless transaction passing through Phase 1 (Wait), Phase 2 (Pre-Commit), and Phase 3 (Commit).
* **[2] Participant Crash (Timeout -> Global Abort):** Demonstrates the Coordinator's fault-tolerance. The `Car` node crashes early, triggering a Coordinator timeout and a safe `GLOBAL_ABORT`.
* **[3] Network Partition - Early Crash (Termination -> Abort):** **(The Core Metric)** The Coordinator and `Hotel` are violently disconnected *before* any node enters the buffer zone. The surviving partition (`Flight` & `Car`) executes the Cooperative Termination Protocol, detects mutual "information blindness" (All `READY`), and autonomously decides to safely **ABORT**.
* **[4] Network Partition - Late Crash (Termination -> Commit):** **(The 3PC Advantage)** The Coordinator crashes *after* moving `Flight` to `PRECOMMIT`. During peer-to-peer termination, `Car` detects `Flight`'s buffer state (The Undeniable Proof), bypassing the blocking state and autonomously deciding to **COMMIT**.

---

## 📁 System Logs (WAL)

During execution, the system dynamically generates log files in the `logs/` directory (e.g., `COORDINATOR_log.json`, `Hotel_log.json`). These files act as the physical storage for the simulated Local Recovery Managers, demonstrating the strictly enforced Write-Ahead Logging mechanism: state transitions are forced/flushed to disk *before* any network transmission occurs.

---

## 📚 References

* **Özsu, M. T., & Valduriez, P.** - *Principles of Distributed Database Systems*. (Theoretical foundation for the 3PC State Machine, Skeen's Non-Blocking Rules, and the Cooperative Termination Protocol).