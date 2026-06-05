<div align="center">

# 🚀 Quant Terminal v11 PRO
**High-Frequency Crypto & Commodities Trading Engine**

[![FastAPI](https://img.shields.io/badge/FastAPI-005571?style=for-the-badge&logo=fastapi)](https://fastapi.tiangolo.com/)
[![React](https://img.shields.io/badge/React-20232A?style=for-the-badge&logo=react&logoColor=61DAFB)](https://reactjs.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-316192?style=for-the-badge&logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![WebSockets](https://img.shields.io/badge/WebSockets-010101?style=for-the-badge&logo=socket.io&logoColor=white)](#)
[![TailwindCSS](https://img.shields.io/badge/Tailwind_CSS-38B2AC?style=for-the-badge&logo=tailwind-css&logoColor=white)](https://tailwindcss.com/)

An advanced, full-stack trading platform simulating the core mechanics of modern financial exchanges. Engineered for zero-latency execution, real-time market data synchronization, and rigorous portfolio management.

</div>

<br/>

> **⚡ v11 PRO Update: Optimized Market Dynamics**
> The Market Maker Bots have been upgraded with a throttled event loop (`3.0s - 5.0s` execution cycles) and Gaussian (Normal) distribution logic for price generation. This ensures realistic market volatility, stable order book rebalancing, and prevents WebSocket congestion.

---

## 📖 Table of Contents
1. [System Architecture](#-system-architecture)
2. [Core Engineering Features](#-core-engineering-features)
3. [Algorithmic Performance](#-algorithmic-performance)
4. [Tech Stack](#-tech-stack)
5. [Local Setup & Installation](#-local-setup--installation)
6. [API Reference](#-api-reference)

---

## 🧠 System Architecture

The backbone of Quant Terminal is a highly optimized, in-memory **Heap-Based Matching Engine**. 

**🔄 Order Execution Workflow:**
1. **Client / UI** ➔ Sends a `Place Order` request via REST API.
2. **FastAPI Backend** ➔ Validates user wallet balance and margin requirements.
3. **Matching Engine** ➔ Routes the order to the correct memory heap:
   * 🟢 **BUY Orders (Bids):** Stored in a **Max-Heap** *(Highest bidder gets priority)*.
   * 🔴 **SELL Orders (Asks):** Stored in a **Min-Heap** *(Cheapest seller gets priority)*.
4. **Price-Time Execution** ➔ Engine instantly matches orders when `Bid ≥ Ask`.
5. **Settlement & Broadcast** ➔ Saves the trade to the PostgreSQL ledger and instantly streams live tick data via **WebSockets** back to the UI.

## ✨ Core Engineering Features

### 1. High-Speed Order Matching
* **O(log n) Execution:** Utilizes Python's native `heapq` for ultra-fast order placement and matching.
* **Smart Order Routing:** Fully supports Partial Fills, Full Fills, Market Orders, and Limit Orders.
* **Priority Queues:** BUY orders use a Max-Heap (highest bidder wins), while SELL orders use a Min-Heap (cheapest seller wins).

### 2. Autonomous Market Makers (Bots)
* **Gaussian Pricing:** Bots inject liquidity using a normal distribution (`random.gauss`) with a 5% standard deviation to simulate natural market chart patterns.
* **Inventory Management:** Auto-balancing logic forces bots to switch between BUY/SELL strategies based on their current asset holdings.

### 3. Institutional Portfolio Engine
* **Realized PnL:** Precisely calculates actual booked profits: `(Avg Sell - Avg Buy) × Units Sold`.
* **Unrealized PnL:** Live calculation of holding values against the Last Traded Price (LTP).
* **Strict Margin Validation:** Prevents naked shorting and ensures sufficient wallet balances before allowing order entry.

### 4. Real-Time WebSocket Streaming
* Zero-polling architecture. Instantly broadcasts JSON payloads for `TRADE`, `BOT_ORDER`, and `UPDATE_BOOK` events to all connected UI clients.

---

## 📊 Algorithmic Performance

The underlying data structures bypass standard database constraints during live trading, handling high-frequency loads purely in-memory before async database commits.

| Operation | Time Complexity | Implementation Detail |
| :--- | :---: | :--- |
| **Place Order** | **O(log n)** | Heap insertion maintains binary tree properties. |
| **Match Orders** | **O(log n)** | Extracting root node and executing partial/full fills. |
| **Top Bid/Ask Lookup**| **O(1)** | Peeking at the 0th index of the active list/heap. |
| **Trade Settlement** | **O(1)** | Wallet and holding recalculations post-execution. |

---

## 🛠 Tech Stack

**Frontend:**
* **React.js** (Vite + Functional Components)
* **Tailwind CSS** (Responsive, High-End UI)
* **Native WebSockets API** & **Axios**

**Backend:**
* **Python 3.9+ & FastAPI** (Asynchronous API design)
* **SQLAlchemy ORM** (Database management)
* **JWT & Passlib** (Secure authentication & Bcrypt hashing)

**Database:**
* **PostgreSQL** (Persistent ledger for Users, Orders, and Trades)

---

## 🚀 Local Setup & Installation

### Prerequisites
* Python 3.9+
* Node.js & npm
* PostgreSQL (Running locally or via cloud)

### 1. Backend Setup
```bash
# Navigate to backend directory
cd backend

# Create and activate virtual environment
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Create environment variables (.env file)
echo "DATABASE_URL=postgresql://user:password@localhost:5432/trading_db" > .env
echo "SECRET_KEY=super_secret_quant_engine_v11" >> .env

# Start the Trading Engine (Starts on Port 8000)
uvicorn trading_engine:app --reload --port 8000
```
*(Note: The system automatically seeds the database with initial Products, Traders, and Bots on startup).*

### 2. Frontend Setup
```bash
# Navigate to frontend directory
cd frontend

# Install Node modules
npm install

# Start the Vite development server
npm run dev
```

---

## 📡 API Reference

### REST Endpoints (Requires JWT)
| Method | Endpoint | Description |
| :--- | :--- | :--- |
| `POST` | `/signup` & `/login` | User authentication and JWT generation. |
| `GET`  | `/products` | Retrieves asset list and live 24h % change. |
| `GET`  | `/order-book/{pid}` | Returns Top 15 Bids/Asks for the visual order book. |
| `POST` | `/place-order` | Submits BUY/SELL orders (Market or Limit). |
| `GET`  | `/portfolio` | Calculates complex Realized/Unrealized PnL. |

### WebSocket Connections
| Protocol | Endpoint | Description |
| :--- | :--- | :--- |
| `WS` | `/ws` | Subscribes to the live execution and order book feed. |

---

<div align="center">
<i>Architected & Developed by Shiwansh Singh</i><br>
<i>Demonstrating advanced algorithmic problem solving and scalable system design.</i>
</div>
