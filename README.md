# 3-Phase Commit (3PC) Simulator: Multi-Resource Reservation

**Team:** Distributed Dynamics  
**Member:** Đinh Việt Hoàng  
**Course:** Distributed Database Systems  
**Project ID:** #34 - Reliability and Commit Protocols  

---

## 📖 Project Overview
The standard 2-Phase Commit (2PC) protocol is inherently blocking. If the Coordinator fails while participants are in the `READY` state, the entire system is paralyzed. 

This project is a Python-based simulator for the **3-Phase Commit (3PC) protocol**, designed to solve this blocking vulnerability. It models a distributed travel reservation ecosystem (Hotel, Flight, Car) coordinating with a Transaction Manager. The system strictly adheres to the theoretical state machines and **Cooperative Termination Protocol** defined by *Özsu & Valduriez*. 

![State Machine Diagram](assets/state_machine.png)
*(Placeholder: Add your State Machine Diagram here)*

---

## 🛠️ Tech Stack & Architecture
- **Language:** Python 3.x (Object-Oriented Design)
- **Concurrency:** `multiprocessing` library to create truly isolated processes for the Coordinator and each Participant.
- **Network Simulation:** `multiprocessing.Manager().Queue()` for Inter-Process Communication (IPC) to simulate asynchronous network routing, polling, and artificial latency.
- **Durability:** Write-Ahead Logging (WAL) utilizing local `.json` files to simulate non-volatile physical disk storage.

![Architecture Diagram](assets/architecture.png)
*(Placeholder: Add your Architecture Diagram here)*

---

## 🚀 Installation & Usage

### 1. Prerequisites
Ensure you have Python 3.8+ installed. No external third-party libraries are required (only Python standard libraries).

### 2. Clone the Repository
```bash
git clone <YOUR-GITHUB-REPO-LINK-HERE>
cd 3pc-simulator