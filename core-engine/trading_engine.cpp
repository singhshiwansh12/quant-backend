/*
 * =====================================================
 * MULTITHREADED ORDER BOOK — C++ Concurrency Demo
 * =====================================================
 *
 * WHAT THIS TEACHES:
 * - std::thread        → OS-level threads (like processes, but lighter)
 * - std::mutex         → Mutual Exclusion lock (only 1 thread at a time)
 * - std::lock_guard    → RAII wrapper — auto-releases lock when scope ends
 * - std::condition_variable → Thread "sleeps" until signal arrives
 * - std::queue         → Shared data structure between threads
 *
 * ARCHITECTURE:
 * Producer Thread → [Shared Queue] → Consumer Thread
 * ↑
 * mutex protects this
 */

#include <iostream>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <queue>
#include <vector>
#include <string>
#include <chrono>
#include <random>
#include <atomic>
#include <map>
#include <iomanip>

// ─────────────────────────────────────────────
//  ORDER STRUCT
//  Represents one Buy or Sell order
// ─────────────────────────────────────────────
struct Order {
    int    id;
    std::string type;   // "BUY" or "SELL"
    double price;
    int    quantity;

    std::string to_string() const {
        return "[Order #" + std::to_string(id) + " | " + type
             + " | Price: " + std::to_string((int)price)
             + " | Qty: " + std::to_string(quantity) + "]";
    }
};

// ─────────────────────────────────────────────
//  ORDER BOOK CLASS
//  Thread-safe queue shared between threads
// ─────────────────────────────────────────────
class OrderBook {
private:
    std::queue<Order>        pending_orders;   // Shared queue
    std::map<int, Order>     buy_orders;       // Active BUY side
    std::map<int, Order>     sell_orders;      // Active SELL side

    // MUTEX = Binary lock. Think of it as a "bathroom key".
    // Only ONE thread can hold it at a time.
    std::mutex               mtx;

    // CONDITION VARIABLE = A "wait here" signal system.
    // Consumer sleeps when queue is empty.
    // Producer wakes it up when new order arrives.
    std::condition_variable cv;

    std::atomic<bool>        done{false};      // Signals when producer is finished
    int                      trades_executed{0};
    double                   total_volume{0.0};

public:

    // ─── PRODUCER calls this ───
    // Adds a new order to the shared queue
    void add_order(const Order& order) {
        // lock_guard = auto-lock + auto-unlock when function scope ends
        // This is RAII — Resource Acquisition Is Initialization
        // Even if an exception throws, lock is ALWAYS released. No deadlock.
        {
            std::lock_guard<std::mutex> lock(mtx);
            pending_orders.push(order);
            std::cout << "\033[34m[PRODUCER]\033[0m Added " << order.to_string() << "\n";
        }
        // OUTSIDE the lock — notify consumer that data is ready
        cv.notify_one();
    }

    // ─── CONSUMER calls this ───
    // Processes orders from the shared queue
    void process_orders() {
        while (true) {
            Order order;

            {
                // unique_lock is needed for condition_variable (lock_guard won't work here)
                std::unique_lock<std::mutex> lock(mtx);

                // cv.wait() does 3 things atomically:
                //   1. Releases the lock (so producer can add orders)
                //   2. Suspends this thread (sleeps — no CPU waste)
                //   3. When notified, re-acquires the lock and checks condition
                cv.wait(lock, [this]() {
                    return !pending_orders.empty() || done.load();
                });

                // If done AND queue is empty, we're finished
                if (pending_orders.empty() && done.load()) break;

                // Take one order from the front
                order = pending_orders.front();
                pending_orders.pop();
            }
            // Lock released here — we process WITHOUT holding the lock
            // This is important: never do slow work while holding a mutex!

            match_order(order);
        }
        print_summary();
    }

    // Simple matching engine logic
    // BUY matches against lowest-priced SELL, and vice versa
    void match_order(const Order& order) {
        std::lock_guard<std::mutex> lock(mtx);

        if (order.type == "BUY") {
            // Find a SELL order at or below this BUY price
            for (auto it = sell_orders.begin(); it != sell_orders.end(); ++it) {
                if (it->second.price <= order.price) {
                    double exec_price = it->second.price;
                    int    exec_qty   = std::min(order.quantity, it->second.quantity);

                    trades_executed++;
                    total_volume += exec_price * exec_qty;

                    std::cout << "\033[32m[MATCH ✓]\033[0m  BUY #" << order.id
                              << " matched with SELL #" << it->first
                              << " @ ₹" << exec_price
                              << " × " << exec_qty << " units\n";

                    sell_orders.erase(it);
                    return;
                }
            }
            // No match — add to BUY side
            buy_orders[order.id] = order;
            std::cout << "\033[33m[QUEUED]\033[0m   BUY #" << order.id
                      << " waiting @ ₹" << order.price << "\n";

        } else { // SELL
            for (auto it = buy_orders.begin(); it != buy_orders.end(); ++it) {
                if (it->second.price >= order.price) {
                    double exec_price = order.price;
                    int    exec_qty   = std::min(order.quantity, it->second.quantity);

                    trades_executed++;
                    total_volume += exec_price * exec_qty;

                    std::cout << "\033[32m[MATCH ✓]\033[0m  SELL #" << order.id
                              << " matched with BUY #" << it->first
                              << " @ ₹" << exec_price
                              << " × " << exec_qty << " units\n";

                    buy_orders.erase(it);
                    return;
                }
            }
            sell_orders[order.id] = order;
            std::cout << "\033[33m[QUEUED]\033[0m   SELL #" << order.id
                      << " waiting @ ₹" << order.price << "\n";
        }
    }

    void signal_done() {
        done.store(true);
        cv.notify_all();   // Wake ALL waiting consumers
    }

    void print_summary() {
        std::cout << "\n\033[36m━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\033[0m\n";
        std::cout << "\033[36m         TRADING SESSION SUMMARY\033[0m\n";
        std::cout << "\033[36m━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\033[0m\n";
        std::cout << "  Trades Executed : " << trades_executed << "\n";
        std::cout << "  Total Volume    : ₹" << std::fixed << std::setprecision(2)
                  << total_volume << "\n";
        std::cout << "  Unmatched BUYs  : " << buy_orders.size() << "\n";
        std::cout << "  Unmatched SELLs : " << sell_orders.size() << "\n";
        std::cout << "\033[36m━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\033[0m\n";
    }
};

// ─────────────────────────────────────────────
//  PRODUCER FUNCTION
//  Runs in its own thread — generates random orders
// ─────────────────────────────────────────────
void producer(OrderBook& book, int num_orders) {
    std::mt19937 rng(42);  // Seeded random number generator
    std::uniform_real_distribution<double> price_dist(95.0, 105.0);
    std::uniform_int_distribution<int>     qty_dist(1, 100);
    std::uniform_int_distribution<int>     type_dist(0, 1);

    for (int i = 1; i <= num_orders; i++) {
        Order o;
        o.id       = i;
        o.type     = (type_dist(rng) == 0) ? "BUY" : "SELL";
        o.price    = std::round(price_dist(rng) * 100) / 100.0;
        o.quantity = qty_dist(rng);

        book.add_order(o);

        // Simulate network/IO delay between orders (50ms)
        // In OS terms: this thread gets context-switched out here
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }

    book.signal_done();
    std::cout << "\033[34m[PRODUCER]\033[0m All orders sent. Signaling done.\n";
}

// ─────────────────────────────────────────────
//  MAIN — Creates 2 threads and joins them
// ─────────────────────────────────────────────
int main() {
    std::cout << "\033[36m━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\033[0m\n";
    std::cout << "\033[36m   MULTITHREADED ORDER BOOK — DEMO\033[0m\n";
    std::cout << "\033[36m━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\033[0m\n\n";

    OrderBook book;

    // std::thread creates an OS-level thread
    // The OS scheduler decides when each thread gets CPU time (context switching)
    std::thread producer_thread(producer, std::ref(book), 12);

    // Consumer runs in THIS (main) thread
    // Both threads now run CONCURRENTLY
    book.process_orders();

    // join() = "wait for producer_thread to finish before exiting"
    // Without this: main() exits, OS kills all threads → undefined behavior
    producer_thread.join();

    std::cout << "\n\033[32mSimulation complete.\033[0m\n";
    return 0;
}
