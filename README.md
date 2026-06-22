<div align="center">

# 🚀 Realtime Trading Engine Simulator
**High-Frequency Multithreaded Crypto & Commodities Exchange**

[![C++](https://img.shields.io/badge/C++-00599C?style=for-the-badge&logo=c%2B%2B&logoColor=white)](#)
[![FastAPI](https://img.shields.io/badge/FastAPI-005571?style=for-the-badge&logo=fastapi)](https://fastapi.tiangolo.com/)
[![React](https://img.shields.io/badge/React-20232A?style=for-the-badge&logo=react&logoColor=61DAFB)](https://reactjs.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-316192?style=for-the-badge&logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![WebSockets](https://img.shields.io/badge/WebSockets-010101?style=for-the-badge&logo=socket.io&logoColor=white)](#)

An advanced, full-stack trading platform simulating the core mechanics of modern financial exchanges. Engineered with a polyglot architecture featuring a highly concurrent **C++ Matching Engine** for zero-latency execution, integrated with a **FastAPI** distributed backend and real-time WebSocket data synchronization.

<br/>

![Quant Terminal Dashboard](https://github.com/user-attachments/assets/9d436f84-2c0e-4002-ad34-8557388dc41d)

</div>

<br/>

> **⚡ Core System Upgrade: C++ Multithreading Migration**
> The core order matching logic has been migrated to a dedicated C++ microservice (`/core-engine`). This utilizes OS-level threads, `std::mutex`, and `std::condition_variable` to handle extreme high-frequency concurrent order processing with zero data races.

---

## 📖 Table of Contents
1. [System Architecture](#-system-architecture)
2. [Core Engineering Features](#-core-engineering-features)
3. [Algorithmic Performance](#-algorithmic-performance)
4. [Tech Stack](#-tech-stack)
5. [Local Setup & Installation](#-local-setup--installation)

---

## 🧠 System Architecture

The backbone of the simulator is a decoupled, highly optimized architecture separating the high-speed matching logic from the API routing layer.

**🔄 Order Execution Workflow:**
1. **API Gateway (FastAPI)** ➔ Validates user wallet balance, margin requirements, and JWT authentication.
2. **Core Matching Engine (C++)** ➔ Orders are pushed to a thread-safe shared queue.
   * 🟢 **Consumer Threads:** Instantly process pending orders locked by `std::mutex` to prevent race conditions.
   * 🔴 **Condition Variables:** Ensure threads sleep gracefully (`cv.wait()`) when queues are empty, preventing CPU spin-locking.
3. **Price-Time Execution** ➔ Engine instantly matches orders using continuous limit order book logic.
4. **Settlement & Broadcast** ➔ Saves the trade to the PostgreSQL ACID-compliant ledger and instantly streams live tick data via **WebSockets** back to the UI.

---

## ✨ Core Engineering Features

### 1. Highly Concurrent C++ Matching Engine
* **Thread-Safety:** Engineered using `std::lock_guard` and `std::unique_lock` to manage shared memory resources safely during simultaneous BUY/SELL surges.
* **Low-Latency Queues:** Bypasses standard database constraints during live trading, handling high-frequency loads purely in-memory.

### 2. Autonomous Market Makers (Bots)
* **Gaussian Pricing:** Python-based bots inject liquidity using a normal distribution (`random.gauss`) with a 5% standard deviation to simulate natural market volatility and chart patterns.
* **Throttled Event Loop:** Optimized bot execution cycles (`3.0s - 5.0s`) to ensure stable order book rebalancing and prevent WebSocket congestion.

### 3. Institutional Portfolio Engine
* **Realized PnL:** Precisely calculates actual booked profits: `(Avg Sell - Avg Buy) × Units Sold`.
* **Strict Margin Validation:** Prevents naked shorting and ensures sufficient wallet balances before allowing order entry.

### 4. Real-Time WebSocket Streaming
* Zero-polling architecture. Instantly broadcasts JSON payloads for `TRADE`, `BOT_ORDER`, and `UPDATE_BOOK` events to all connected UI clients without page refreshes.

---

## 📊 Algorithmic Performance

| Operation | Time Complexity | Implementation Detail |
| :--- | :---: | :--- |
| **Concurrent Order Injection** | **O(1)** | Lock acquisition and thread-safe queue push. |
| **Order Matching** | **O(n)** | Traversal of active limits with early termination on match. |
| **Thread Context Switching**| **Microseconds** | Managed by OS scheduler via `std::thread`. |
| **Trade Settlement** | **O(1)** | Wallet and holding recalculations post-execution. |

---

## 🛠 Tech Stack

**Core Systems & Backend:**
* **C++11/14** (`<thread>`, `<mutex>`, OS-level Concurrency)
* **Python 3.9+ & FastAPI** (Asynchronous API design & Routing)
* **SQLAlchemy ORM** (Database transaction management)
* **JWT & Passlib** (Secure authentication & Bcrypt hashing)

**Database & Streaming:**
* **PostgreSQL** (Persistent ledger for Users, Orders, and Trades)
* **WebSockets API** (Live market data broadcasting)

**Frontend:**
* **React.js** (Vite + Functional Components)
* **Tailwind CSS** (Responsive, High-End UI)

---

## 🚀 Local Setup & Installation

### 1. Compile Core Matching Engine (C++)
```bash
# Navigate to core engine directory
cd core-engine

# Compile with pthread support
g++ -std=c++11 -pthread trading_engine.cpp -o engine

# Run the multithreaded simulator
./engine


# Navigate to backend directory
cd backend

# Create and activate virtual environment
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Create environment variables (.env file)
echo "DATABASE_URL=postgresql://user:password@localhost:5432/trading_db" > .env
echo "SECRET_KEY=super_secret_quant_engine" >> .env

# Start the Trading API (Starts on Port 8000)
uvicorn trading_engine:app --reload --port 8000
# Navigate to frontend directory
cd frontend

# Install Node modules
npm install

# Start the Vite development server
npm run dev
