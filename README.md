# 3-Phase Commit (3PC) Simulator: Multi-Resource Reservation

**Team:** Distributed Dynamics
**Member:** Đinh Việt Hoàng
**Course:** Distributed Database Systems
**Project ID:** #34 - Reliability and Commit Protocols

---

## 📖 Project Overview

The standard 2-Phase Commit (2PC) protocol is inherently blocking. If the Coordinator fails while participants are in the `READY` state, the entire system becomes blocked until recovery occurs.

This project is a Python-based simulator of the **3-Phase Commit (3PC) protocol**, designed to eliminate this blocking vulnerability. It models a distributed travel reservation ecosystem consisting of Hotel, Flight, and Car reservation services coordinated by a Transaction Manager.

The implementation follows the theoretical state machines and Cooperative Termination Protocol described in distributed database literature.

<img width="1187" height="781" alt="Screenshot 2026-06-01 025831" src="https://github.com/user-attachments/assets/574e0c3e-8eae-471c-94de-69969921c2cc" />


---

## 🛠️ Tech Stack

### Core Technologies

* **Python 3.x**
* **Object-Oriented Programming (OOP)**
* **Multiprocessing** for isolated Coordinator and Participant processes
* **Inter-Process Communication (IPC)** using `multiprocessing.Queue()`
* **Write-Ahead Logging (WAL)** using JSON-based persistent logs

---

## 🏗️ Architecture

### Architecture Components

* **Coordinator Process**

  * Acts as the Transaction Manager.
  * Executes the 3PC protocol phases.

* **Participant Processes**

  * Hotel Service
  * Flight Service
  * Car Service

* **Network Layer**

  * Simulated through IPC message queues.
  * Supports message passing, artificial latency, and network partitions.

* **Persistence Layer**

  * JSON-based WAL logs.
  * Provides durable state recovery.


<img width="879" height="280" alt="image" src="https://github.com/user-attachments/assets/5d7b266e-d7ae-488a-9dc3-ee07a8c9231a" />


---

## 📂 Project Structure


```text
3pc-simulator/
│
├── core/                  # Core protocol logic
│   ├── __init__.py        # Python package initializer
│   ├── coordinator.py     # Transaction Manager implementation
│   ├── logger.py          # Write-Ahead Logging (WAL) logic
│   ├── messages.py        # Data Transfer Objects (DTO) for networking
│   ├── participant.py     # Local Recovery Manager implementation
│   └── states.py          # Enums for 3PC States (WAIT, READY, PRECOMMIT, etc.)
│
├── datasets/              # Simulated database inventories
│   ├── car_db.json
│   ├── flight_db.json
│   └── hotel_db.json
│
├── logs/                  # Auto-generated persistent WAL storage
│   ├── Car_log.json
│   ├── COORDINATOR_log.json
│   ├── Flight_log.json
│   └── Hotel_log.json
│
├── init_db.py             # Utility script to initialize/reset datasets
├── main.py                # Entry point & Interactive Menu for 4 Scenarios
└── README.md              # Project documentation
```

---

## 🚀 Installation & Usage

### 1. Prerequisites

Install Python 3.8 or later.

No external dependencies are required.

### 2. Clone the Repository

```bash
git clone <YOUR-GITHUB-REPOSITORY-LINK>
cd 3pc-simulator
```

### 3. Run the Simulator

```bash
python main.py
```


<img width="1052" height="451" alt="image" src="https://github.com/user-attachments/assets/5743d038-7e1d-4b2e-8b04-aff37a7e3209" />

---

## 🎬 Simulation Scenarios

The simulator contains four fault-tolerance demonstrations.

### Scenario 1 — Happy Path

* Successful distributed transaction.
* Flow: `WAIT → PRECOMMIT → COMMIT`
* All participants vote YES and the transaction commits successfully.

### Scenario 2 — Participant Crash

* The `Car` participant crashes before voting.
* Coordinator timeout triggers `GLOBAL_ABORT`.
* Demonstrates fault tolerance through safe transaction abortion.

### Scenario 3 — Early Network Partition

* Coordinator and `Hotel` become isolated before any participant reaches `PRECOMMIT`.
* Remaining participants execute the Cooperative Termination Protocol.
* Observed state: all surviving nodes are in `READY`.
* Final decision: **ABORT**.

This demonstrates safe autonomous recovery under uncertainty.

### Scenario 4 — Late Network Partition

* Coordinator crashes after moving `Flight` into `PRECOMMIT`.
* During termination, another participant discovers a peer already in `PRECOMMIT`.
* Observed state:

  * `Flight = PRECOMMIT`
  * `Car = READY`
* Final decision: **COMMIT**.

This demonstrates the non-blocking advantage of 3PC over 2PC.

---

## 📁 Write-Ahead Logs (WAL)

During execution, the simulator automatically generates log files in the `logs/` directory:

```text
logs/
├── COORDINATOR_log.json
├── Hotel_log.json
├── Flight_log.json
└── Car_log.json
```

Each state transition is flushed to persistent storage before any network transmission occurs, demonstrating the Write-Ahead Logging principle.

---

## 📚 References

Özsu, M. T., & Valduriez, P.

*Principles of Distributed Database Systems*

This project follows the theoretical foundations presented in the book, particularly:

* 3-Phase Commit State Machine
* Cooperative Termination Protocol
* Non-Blocking Commit Rules
* Distributed Transaction Recovery
