# =============================================================================
# trading_engine.py – Quant Terminal v11 PRO
# Run with: uvicorn trading_engine:app --reload --port 8000
# =============================================================================

import json, heapq, random, asyncio
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import FastAPI, Depends, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from pydantic import BaseModel
from sqlalchemy import create_engine, Column, Integer, String, Float, ForeignKey, DateTime
from sqlalchemy.orm import declarative_base, sessionmaker, Session, relationship
from passlib.context import CryptContext
from jose import JWTError, jwt

SECRET_KEY = "super_secret_quant_engine_v11"
ALGORITHM  = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 300

pwd_context   = CryptContext(schemes=["bcrypt"], deprecated="auto")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="login")

# ── ENHANCED DATABASE CONNECTION ─────────────────────────────────────────────
engine = create_engine(
    "sqlite:///./trading_live.db",
    connect_args={"check_same_thread": False, "timeout": 15},
    pool_size=50,
    max_overflow=100
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base         = declarative_base()

# ── MODELS ──────────────────────────────────────────────────────────────────

class DBUser(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, unique=True, index=True)
    hashed_password = Column(String)
    wallet_balance = Column(Float, default=100_000.0)
    role = Column(String, default="user")

class DBProduct(Base):
    __tablename__ = "products"
    pid = Column(Integer, primary_key=True, index=True)
    name = Column(String, unique=True, index=True)
    base_price = Column(Float, default=100.0)

class DBOrder(Base):
    __tablename__ = "orders"
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"))
    product_id = Column(Integer, ForeignKey("products.pid"))
    price = Column(Float)
    quantity = Column(Integer)
    type = Column(String)          # BUY | SELL
    status = Column(String, default="PENDING")  # PENDING | PARTIAL | FILLED | CANCELLED

class DBTrade(Base):
    __tablename__ = "trades"
    id = Column(Integer, primary_key=True, index=True)
    product_id = Column(Integer, ForeignKey("products.pid"))
    price = Column(Float)
    quantity = Column(Integer)
    buyer_id = Column(Integer, ForeignKey("users.id"))
    seller_id = Column(Integer, ForeignKey("users.id"))
    timestamp = Column(DateTime, default=lambda: datetime.now(timezone.utc))

Base.metadata.create_all(bind=engine)

# ── WEBSOCKET MANAGER ────────────────────────────────────────────────────────

class ConnectionManager:
    def __init__(self):
        self.active_connections: list[WebSocket] = []

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.active_connections.append(ws)

    def disconnect(self, ws: WebSocket):
        if ws in self.active_connections:
            self.active_connections.remove(ws)

    async def broadcast(self, msg: str):
        dead = []
        for c in self.active_connections:
            try:
                await c.send_text(msg)
            except:
                dead.append(c)
        for d in dead:
            self.disconnect(d)

manager     = ConnectionManager()
order_books: dict = defaultdict(lambda: {"BUY": [], "SELL": []})
last_prices: dict[int, float] = {}
price_history: dict[int, list] = defaultdict(list)   # pid → last 200 (price, ts) snapshots

# ── SEED DATA ────────────────────────────────────────────────────────────────

PRODUCTS = [
    {"name": "Emeralds",  "base_price": 500.0},
    {"name": "Tomatoes",  "base_price": 50.0},
    {"name": "Gold",      "base_price": 1000.0},
    {"name": "Silver",    "base_price": 300.0},
    {"name": "Coal",      "base_price": 80.0},
]

SEED_USERS = [
    {"name": "admin",   "password": "admin123",  "role": "admin", "wallet": 999_999_999.0},
    {"name": "trader1", "password": "trade123",  "role": "user",  "wallet": 1_000_000.0},
    {"name": "trader2", "password": "trade123",  "role": "user",  "wallet": 1_000_000.0},
]

BOT_PRODUCT_ASSIGNMENTS = {
    "GoldBot_Alpha": "Gold",     "GoldBot_Beta": "Gold",     "GoldBot_Gamma": "Gold",
    "SilverBot_Alpha": "Silver", "SilverBot_Beta": "Silver", "SilverBot_Gamma": "Silver",
    "EmeraldBot_Alpha": "Emeralds", "EmeraldBot_Beta": "Emeralds", "EmeraldBot_Gamma": "Emeralds",
    "TomatoBot_Alpha": "Tomatoes",  "TomatoBot_Beta": "Tomatoes",  "TomatoBot_Gamma": "Tomatoes",
    "CoalBot_Alpha": "Coal",    "CoalBot_Beta": "Coal",     "CoalBot_Gamma": "Coal",
}
BOT_IDS: dict[str, int] = {}

# ── APP SETUP ────────────────────────────────────────────────────────────────

app = FastAPI(title="Quant Terminal v11")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True,
                   allow_methods=["*"], allow_headers=["*"])

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def get_current_user(token: str = Depends(oauth2_scheme), db: Session = Depends(get_db)) -> DBUser:
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid or expired token")
    user = db.query(DBUser).filter(DBUser.id == int(payload.get("sub"))).first()
    if not user:
        raise HTTPException(status_code=401, detail="User not found")
    return user

def get_user_holdings(db: Session, user_id: int, product_id: int) -> int:
    bought = sum(t[0] for t in db.query(DBTrade.quantity)
                 .filter(DBTrade.buyer_id == user_id, DBTrade.product_id == product_id).all()) or 0
    sold   = sum(t[0] for t in db.query(DBTrade.quantity)
                 .filter(DBTrade.seller_id == user_id, DBTrade.product_id == product_id).all()) or 0
    return bought - sold

def get_active_sell_commitments(db: Session, user_id: int, product_id: int) -> int:
    return sum(q[0] for q in db.query(DBOrder.quantity)
               .filter(DBOrder.user_id == user_id, DBOrder.product_id == product_id,
                       DBOrder.type == "SELL",
                       DBOrder.status.in_(["PENDING", "PARTIAL"])).all()) or 0

# ── STARTUP ──────────────────────────────────────────────────────────────────

@app.on_event("startup")
async def startup():
    db = SessionLocal()
    try:
        for p in PRODUCTS:
            if not db.query(DBProduct).filter(DBProduct.name == p["name"]).first():
                db.add(DBProduct(name=p["name"], base_price=p["base_price"]))
        db.commit()

        for p in db.query(DBProduct).all():
            last_prices[p.pid] = p.base_price

        for u in SEED_USERS:
            if not db.query(DBUser).filter(DBUser.name == u["name"]).first():
                db.add(DBUser(name=u["name"], hashed_password=pwd_context.hash(u["password"]),
                               role=u["role"], wallet_balance=u["wallet"]))
        db.commit()

        admin = db.query(DBUser).filter(DBUser.name == "admin").first()
        for bot_name, product_name in BOT_PRODUCT_ASSIGNMENTS.items():
            bot = db.query(DBUser).filter(DBUser.name == bot_name).first()
            if not bot:
                bot = DBUser(name=bot_name, hashed_password=pwd_context.hash("bot_secret"),
                              role="user", wallet_balance=10_000_000.0)
                db.add(bot); db.commit(); db.refresh(bot)
            BOT_IDS[bot_name] = bot.id
            product = db.query(DBProduct).filter(DBProduct.name == product_name).first()
            if get_user_holdings(db, bot.id, product.pid) < 500:
                db.add(DBTrade(product_id=product.pid, price=product.base_price,
                               quantity=2000, buyer_id=bot.id, seller_id=admin.id))
        db.commit()
    finally:
        db.close()

    for i, (bot_name, product_name) in enumerate(BOT_PRODUCT_ASSIGNMENTS.items()):
        asyncio.create_task(single_bot_loop(bot_name, product_name, initial_delay=1 + i * 0.1))

# ── BOT ENGINE ───────────────────────────────────────────────────────────────

async def single_bot_loop(bot_name: str, product_name: str, initial_delay: float):
    await asyncio.sleep(initial_delay)
    while True:
        try:
            await run_bot_cycle(bot_name, product_name)
        except Exception as e:
            print(f"[{bot_name} ERROR] {e}")
        await asyncio.sleep(random.uniform(0.5, 1.5))

async def run_bot_cycle(bot_name: str, product_name: str):
    db = SessionLocal()
    try:
        bot_id  = BOT_IDS.get(bot_name)
        product = db.query(DBProduct).filter(DBProduct.name == product_name).first()
        if not bot_id or not product:
            return
        pid  = product.pid
        base = last_prices.get(pid, product.base_price)

        sigma     = base * 0.05
        raw_price = random.gauss(base, sigma)
        raw_price = max(base * 0.80, min(base * 1.20, raw_price))
        price     = round(raw_price, 2)
        qty       = random.randint(1, 5)

        bot_user = db.query(DBUser).filter(DBUser.id == bot_id).first()
        avail    = get_user_holdings(db, bot_id, pid) - get_active_sell_commitments(db, bot_id, pid)

        if avail < 100:
            otype = "BUY"
        elif avail > 5000:
            otype = "SELL"
        else:
            otype = random.choice(["BUY", "SELL"])

        if otype == "BUY"  and bot_user.wallet_balance < price * qty: return
        if otype == "SELL" and avail < qty: return

        order = DBOrder(user_id=bot_id, product_id=pid, price=price, quantity=qty, type=otype)
        db.add(order); db.commit(); db.refresh(order)

        if otype == "BUY":
            heapq.heappush(order_books[pid]["BUY"],  (-price, order.id, qty, bot_id))
        else:
            heapq.heappush(order_books[pid]["SELL"], (price, order.id, qty, bot_id))

        trades = match_orders_sync(db, pid)
        for t in trades:
            await manager.broadcast(json.dumps(t))

        if random.random() > 0.6:
            await manager.broadcast(json.dumps({
                "type": "BOT_ORDER", "bot": bot_name, "product": product_name,
                "product_id": pid, "order_type": otype, "price": price, "qty": qty
            }))
            await manager.broadcast(json.dumps({"type": "UPDATE_BOOK"}))
    finally:
        db.close()

# ── ORDER MATCHING ───────────────────────────────────────────────────────────

def match_orders_sync(db: Session, pid: int) -> list:
    executed_trades = []
    buys  = order_books[pid]["BUY"]
    sells = order_books[pid]["SELL"]

    while buys and sells:
        # Drain stale buy entries
        while buys:
            top = db.query(DBOrder).filter(DBOrder.id == buys[0][1]).first()
            if top and top.status in ("PENDING", "PARTIAL"): break
            heapq.heappop(buys)
        # Drain stale sell entries
        while sells:
            top = db.query(DBOrder).filter(DBOrder.id == sells[0][1]).first()
            if top and top.status in ("PENDING", "PARTIAL"): break
            heapq.heappop(sells)
        if not buys or not sells: break

        best_buy  = -buys[0][0]
        best_sell =  sells[0][0]
        if best_buy < best_sell: break

        b_neg, b_oid, b_qty, buyer_id  = heapq.heappop(buys)
        s_price, s_oid, s_qty, seller_id = heapq.heappop(sells)
        b_price = -b_neg

        trade_qty   = min(b_qty, s_qty)
        trade_price = round((b_price + s_price) / 2, 2)
        last_prices[pid] = trade_price

        # Record price in in-memory history (keep last 200 points)
        snap = price_history[pid]
        snap.append({"price": trade_price, "ts": datetime.now(timezone.utc).isoformat()})
        if len(snap) > 200:
            price_history[pid] = snap[-200:]

        buyer  = db.query(DBUser).filter(DBUser.id == buyer_id).first()
        seller = db.query(DBUser).filter(DBUser.id == seller_id).first()

        if buyer_id != seller_id:
            if buyer:  buyer.wallet_balance  -= trade_qty * trade_price
            if seller: seller.wallet_balance += trade_qty * trade_price

        db.add(DBTrade(product_id=pid, price=trade_price, quantity=trade_qty,
                       buyer_id=buyer_id, seller_id=seller_id))
        db.query(DBOrder).filter(DBOrder.id.in_([b_oid, s_oid])).update(
            {"status": "FILLED"}, synchronize_session=False)

        if b_qty > trade_qty:
            rem = b_qty - trade_qty
            heapq.heappush(buys, (-b_price, b_oid, rem, buyer_id))
            db.query(DBOrder).filter(DBOrder.id == b_oid).update(
                {"status": "PARTIAL", "quantity": rem}, synchronize_session=False)
        if s_qty > trade_qty:
            rem = s_qty - trade_qty
            heapq.heappush(sells, (s_price, s_oid, rem, seller_id))
            db.query(DBOrder).filter(DBOrder.id == s_oid).update(
                {"status": "PARTIAL", "quantity": rem}, synchronize_session=False)
        db.commit()

        executed_trades.append({
            "type": "TRADE", "product_id": pid, "price": trade_price,
            "quantity": trade_qty,
            "buyer":  buyer.name  if buyer  else "?",
            "seller": seller.name if seller else "?",
            "timestamp": datetime.now(timezone.utc).isoformat()
        })

    return executed_trades

# ── SCHEMAS ──────────────────────────────────────────────────────────────────

class UserRequest(BaseModel):
    name: str
    password: str

class OrderRequest(BaseModel):
    product_id: int
    price: float
    quantity: int
    type: str
    market: bool = False

# ── AUTH ROUTES ───────────────────────────────────────────────────────────────

@app.post("/signup")
def create_user(user: UserRequest, db: Session = Depends(get_db)):
    if db.query(DBUser).filter(DBUser.name == user.name).first():
        raise HTTPException(status_code=400, detail="Username taken")
    db.add(DBUser(name=user.name, hashed_password=pwd_context.hash(user.password),
                   role="user", wallet_balance=100_000.0))
    db.commit()
    return {"message": "Created"}

@app.post("/login")
def login(form: OAuth2PasswordRequestForm = Depends(), db: Session = Depends(get_db)):
    user = db.query(DBUser).filter(DBUser.name == form.username).first()
    if not user or not pwd_context.verify(form.password, user.hashed_password):
        raise HTTPException(status_code=400, detail="Invalid credentials")
    token = jwt.encode(
        {"sub": str(user.id), "exp": datetime.now(timezone.utc) + timedelta(minutes=300)},
        SECRET_KEY, algorithm=ALGORITHM
    )
    return {"access_token": token}

# ── MARKET DATA ROUTES ────────────────────────────────────────────────────────

@app.get("/products")
def get_products(db: Session = Depends(get_db)):
    products = []
    for p in db.query(DBProduct).all():
        lp = last_prices.get(p.pid, p.base_price)
        hist = price_history.get(p.pid, [])
        change_pct = 0.0
        if len(hist) >= 2:
            first = hist[0]["price"]
            change_pct = round((lp - first) / first * 100, 2) if first else 0
        products.append({
            "pid": p.pid, "name": p.name,
            "last_price": lp, "change_pct": change_pct
        })
    return products

@app.get("/order-book/{pid}")
def get_order_book(pid: int):
    buys  = sorted(order_books[pid]["BUY"],  key=lambda x: -x[0])
    sells = sorted(order_books[pid]["SELL"], key=lambda x:  x[0])
    return {
        "bids": [{"price": -b[0], "qty": b[2]} for b in buys[:15]],
        "asks": [{"price":  s[0], "qty": s[2]} for s in sells[:15]],
    }

@app.get("/trade-history/{pid}")
def get_trade_history(pid: int, limit: int = 100, db: Session = Depends(get_db)):
    trades = (db.query(DBTrade).filter(DBTrade.product_id == pid)
              .order_by(DBTrade.id.desc()).limit(limit).all())
    result = []
    for t in trades:
        buyer  = db.query(DBUser).filter(DBUser.id == t.buyer_id).first()
        seller = db.query(DBUser).filter(DBUser.id == t.seller_id).first()
        result.append({
            "id": t.id, "price": t.price, "quantity": t.quantity,
            "buyer":  buyer.name  if buyer  else "?",
            "seller": seller.name if seller else "?",
            "timestamp": t.timestamp.isoformat() if t.timestamp else None
        })
    return result

@app.get("/price-history/{pid}")
def get_price_history(pid: int):
    return price_history.get(pid, [])

# ── ORDER ROUTES ──────────────────────────────────────────────────────────────

@app.post("/place-order")
async def place_order(order: OrderRequest, db: Session = Depends(get_db),
                      current_user: DBUser = Depends(get_current_user)):
    product = db.query(DBProduct).filter(DBProduct.pid == order.product_id).first()
    if not product:
        raise HTTPException(status_code=400, detail="Invalid product")

    # ── INSTANT MARKET ORDER ──
    if order.market:
        cost = order.price * order.quantity
        if order.type == "BUY" and current_user.wallet_balance < cost:
            raise HTTPException(status_code=400, detail="Insufficient funds for Instant Buy")
        if order.type == "SELL":
            avail = (get_user_holdings(db, current_user.id, order.product_id)
                     - get_active_sell_commitments(db, current_user.id, order.product_id))
            if order.quantity > avail:
                raise HTTPException(status_code=400, detail="Insufficient holdings for Instant Sell")

        bot_name = next((bn for bn, pn in BOT_PRODUCT_ASSIGNMENTS.items()
                         if pn == product.name), None)
        bot_id = BOT_IDS.get(bot_name)
        if not bot_id:
            raise HTTPException(status_code=500, detail="No market maker available")

        trade_price = order.price
        last_prices[order.product_id] = trade_price

        if order.type == "BUY":
            current_user.wallet_balance -= cost
            bot_user = db.query(DBUser).filter(DBUser.id == bot_id).first()
            bot_user.wallet_balance += cost
            t = DBTrade(product_id=order.product_id, price=trade_price,
                        quantity=order.quantity, buyer_id=current_user.id, seller_id=bot_id)
        else:
            current_user.wallet_balance += cost
            bot_user = db.query(DBUser).filter(DBUser.id == bot_id).first()
            bot_user.wallet_balance -= cost
            t = DBTrade(product_id=order.product_id, price=trade_price,
                        quantity=order.quantity, buyer_id=bot_id, seller_id=current_user.id)

        db.add(t); db.commit()
        await manager.broadcast(json.dumps({
            "type": "TRADE", "product_id": order.product_id,
            "price": trade_price, "quantity": order.quantity,
            "buyer":  current_user.name if order.type == "BUY"  else bot_name,
            "seller": bot_name           if order.type == "BUY"  else current_user.name,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }))
        return {"message": "Market Order Filled", "price": trade_price}

    # ── LIMIT ORDER ──
    if order.type == "BUY":
        if current_user.wallet_balance < order.price * order.quantity:
            raise HTTPException(status_code=400, detail="Insufficient funds")
    else:
        is_exempt = current_user.role in ("admin", "seller") or current_user.id in BOT_IDS.values()
        if not is_exempt:
            avail = (get_user_holdings(db, current_user.id, order.product_id)
                     - get_active_sell_commitments(db, current_user.id, order.product_id))
            if order.quantity > avail:
                raise HTTPException(status_code=400, detail="Naked shorting not allowed")

    new_order = DBOrder(user_id=current_user.id, product_id=order.product_id,
                        price=order.price, quantity=order.quantity, type=order.type)
    db.add(new_order); db.commit(); db.refresh(new_order)

    if order.type == "BUY":
        heapq.heappush(order_books[order.product_id]["BUY"],
                       (-order.price, new_order.id, order.quantity, current_user.id))
    else:
        heapq.heappush(order_books[order.product_id]["SELL"],
                       (order.price, new_order.id, order.quantity, current_user.id))

    trades = match_orders_sync(db, order.product_id)
    for t in trades:
        await manager.broadcast(json.dumps(t))
    await manager.broadcast(json.dumps({"type": "UPDATE_BOOK"}))
    return {"message": "Limit Order Placed", "order_id": new_order.id}

@app.get("/my-orders")
def get_my_orders(current_user: DBUser = Depends(get_current_user),
                  db: Session = Depends(get_db)):
    orders = (db.query(DBOrder)
              .filter(DBOrder.user_id == current_user.id,
                      DBOrder.status.in_(["PENDING", "PARTIAL"]))
              .order_by(DBOrder.id.desc()).limit(50).all())
    result = []
    for o in orders:
        product = db.query(DBProduct).filter(DBProduct.pid == o.product_id).first()
        lp = last_prices.get(o.product_id, 0)
        result.append({
            "id": o.id,
            "product_name": product.name if product else "?",
            "product_id": o.product_id,
            "price": o.price,
            "quantity": o.quantity,
            "type": o.type,
            "status": o.status,
            "last_price": lp,
            "distance_pct": round((o.price - lp) / lp * 100, 2) if lp else 0
        })
    return result

@app.delete("/orders/{oid}")
async def cancel_order(oid: int, current_user: DBUser = Depends(get_current_user),
                       db: Session = Depends(get_db)):
    order = db.query(DBOrder).filter(DBOrder.id == oid,
                                      DBOrder.user_id == current_user.id).first()
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    if order.status not in ("PENDING", "PARTIAL"):
        raise HTTPException(status_code=400, detail="Order cannot be cancelled")
    db.query(DBOrder).filter(DBOrder.id == oid).update({"status": "CANCELLED"})
    db.commit()
    return {"message": "Order cancelled"}

# ── BOT TRIGGER ROUTE ─────────────────────────────────────────────────────────

@app.post("/trigger-bots/{pid}")
async def trigger_bots_endpoint(pid: int, db: Session = Depends(get_db),
                                 current_user: DBUser = Depends(get_current_user)):
    product = db.query(DBProduct).filter(DBProduct.pid == pid).first()
    if not product:
        raise HTTPException(status_code=404, detail="Product not found")
    bot_names = [bn for bn, pn in BOT_PRODUCT_ASSIGNMENTS.items() if pn == product.name]
    await asyncio.gather(*[run_bot_cycle(bn, product.name) for bn in bot_names],
                          return_exceptions=True)
    return {"message": f"Triggered {len(bot_names)} market makers", "product": product.name}

# ── PORTFOLIO ROUTE ───────────────────────────────────────────────────────────

@app.get("/portfolio")
def get_portfolio(current_user: DBUser = Depends(get_current_user),
                  db: Session = Depends(get_db)):
    holdings = []
    total_realized = 0.0

    for p in db.query(DBProduct).all():
        net_qty = get_user_holdings(db, current_user.id, p.pid)
        lp = last_prices.get(p.pid, p.base_price)

        buys  = db.query(DBTrade).filter(DBTrade.buyer_id  == current_user.id,
                                          DBTrade.product_id == p.pid).all()
        sells = db.query(DBTrade).filter(DBTrade.seller_id == current_user.id,
                                          DBTrade.product_id == p.pid).all()

        buy_qty  = sum(t.quantity for t in buys)
        buy_cost = sum(t.price * t.quantity for t in buys)
        avg_buy  = buy_cost / buy_qty if buy_qty else 0.0

        sell_qty     = sum(t.quantity for t in sells)
        sell_revenue = sum(t.price * t.quantity for t in sells)
        avg_sell     = sell_revenue / sell_qty if sell_qty else 0.0

        # Realized P&L: (avg sell - avg buy) × units sold
        realized_pnl = (avg_sell - avg_buy) * sell_qty if avg_buy and sell_qty else 0.0
        total_realized += realized_pnl

        val = net_qty * lp
        inv = avg_buy * net_qty if avg_buy else 0.0
        unrealized_pnl = val - inv
        pnl_pct = (unrealized_pnl / inv * 100) if inv else 0.0

        holdings.append({
            "product_id":    p.pid,
            "product_name":  p.name,
            "quantity":      net_qty,
            "last_price":    lp,
            "avg_buy_price": avg_buy,
            "value":         val,
            "invested":      inv,
            "unrealized_pnl": unrealized_pnl,
            "realized_pnl":   realized_pnl,
            "pnl_pct":       pnl_pct,
            "sell_qty":      sell_qty,
            "sell_revenue":  sell_revenue,
        })

    h_val = sum(h["value"] for h in holdings)
    total_unrealized = sum(h["unrealized_pnl"] for h in holdings)

    return {
        "name":             current_user.name,
        "role":             current_user.role,
        "wallet":           current_user.wallet_balance,
        "holdings":         holdings,
        "holdings_value":   h_val,
        "total_unrealized": total_unrealized,
        "total_realized":   total_realized,
        "total_pnl":        total_unrealized + total_realized,
        "net_worth":        current_user.wallet_balance + h_val,
    }

# ── WEBSOCKET ─────────────────────────────────────────────────────────────────

@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await manager.connect(ws)
    try:
        while True:
            await asyncio.sleep(30)
            await ws.send_text(json.dumps({"type": "PING"}))
    except:
        manager.disconnect(ws)
