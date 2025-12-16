import asyncio
import hashlib
import hmac
import time
import json
import os
import sys
import math
import logging
import base64
from datetime import datetime

import httpx
from dotenv import load_dotenv
from sqlalchemy import create_engine, Column, Integer, String, Float, Boolean, DateTime
from sqlalchemy.orm import sessionmaker, declarative_base

# --- KONFIGURATION & LOGGING ---
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    stream=sys.stdout
)

# API Keys
API_KEY = os.getenv("BITGET_API_KEY")
SECRET_KEY = os.getenv("BITGET_API_SECRET")
PASSPHRASE = os.getenv("BITGET_PASSWORD")
DATABASE_URL = os.getenv("DATABASE_URL")
# WICHTIG: Dieser Key muss mit dem user_key in deiner DB übereinstimmen!
# Entweder fest setzen oder auch aus Env laden.
CURRENT_USER_KEY = os.getenv("BOT_USER_KEY", API_KEY) 

if not all([API_KEY, SECRET_KEY, PASSPHRASE, DATABASE_URL]):
    logging.critical("Fehlende Umgebungsvariablen. Beende Programm.")
    sys.exit(1)

# Bitget Endpoints
BASE_URL = "https://api.bitget.com"
PLAN_URL = "/api/v2/mix/order/orders-plan-pending"
POSITION_URL = "/api/v2/mix/position/all-position"
CANCEL_PLAN_URL = "/api/v2/mix/order/cancel-plan-order"

# --- DATENBANK SETUP ---
Base = declarative_base()

class TradingSignal(Base):
    __tablename__ = "trading_signals"

    id = Column(Integer, primary_key=True, index=True)
    user_key = Column(String, index=True, nullable=False)
    symbol = Column(String, index=True, nullable=False)
    position_type = Column(String, nullable=False) # 'long' oder 'short'
    
    # Preise
    tp1_price = Column(Float, nullable=True)
    tp2_price = Column(Float, nullable=True)
    tp3_price = Column(Float, nullable=True)
    
    # Status Flags
    tp1_reached = Column(Boolean, default=False)
    tp2_reached = Column(Boolean, default=False)
    tp3_reached = Column(Boolean, default=False)
    
    tp1_order_placed = Column(Boolean, default=False)
    tp2_order_placed = Column(Boolean, default=False)
    tp3_order_placed = Column(Boolean, default=False)

    is_active = Column(Boolean, default=False)

# Engine & Session
try:
    # Fix für postgres:// vs postgresql:// in manchen Cloud-Umgebungen
    if DATABASE_URL.startswith("postgres://"):
        DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)
    
    engine = create_engine(DATABASE_URL, pool_pre_ping=True)
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
except Exception as e:
    logging.critical(f"Datenbankverbindung fehlgeschlagen: {e}")
    sys.exit(1)


# --- BITGET API CLASS ---

class BitgetClient:
    def __init__(self):
        self.client = httpx.AsyncClient(base_url=BASE_URL, timeout=10.0)

    async def close(self):
        await self.client.aclose()

    def _generate_signature(self, method, path, body=""):
        timestamp = str(int(time.time() * 1000))
        message = timestamp + method.upper() + path + body
        hmac_key = SECRET_KEY.encode('utf-8')
        signature = base64.b64encode(hmac.new(hmac_key, message.encode('utf-8'), hashlib.sha256).digest()).decode()
        return timestamp, signature

    def _get_headers(self, method, path, body=""):
        timestamp, signature = self._generate_signature(method, path, body)
        return {
            "Content-Type": "application/json",
            "ACCESS-KEY": API_KEY,
            "ACCESS-SIGN": signature,
            "ACCESS-TIMESTAMP": timestamp,
            "ACCESS-PASSPHRASE": PASSPHRASE,
            "locale": "en-US"
        }

    async def request(self, method, endpoint, params=None, data=None):
        path = endpoint
        body_str = ""
        
        if method == "GET" and params:
            query = httpx.QueryParams(params)
            path += f"?{query}"
        elif method == "POST" and data:
            body_str = json.dumps(data, separators=(',', ':'))

        headers = self._get_headers(method, path, body_str)
        
        try:
            if method == "GET":
                response = await self.client.get(endpoint, params=params, headers=headers)
            else:
                response = await self.client.post(endpoint, json=data, headers=headers)
            
            response.raise_for_status()
            res_json = response.json()
            
            if res_json.get("code") != "00000":
                logging.error(f"API Error {res_json.get('code')}: {res_json.get('msg')}")
                return None
            
            return res_json.get("data")
        except Exception as e:
            logging.error(f"Request failed: {e}")
            return None

    async def get_open_positions(self):
        """Holt alle offenen Positionen."""
        # marginCoin USDT ist Standard für USDT-Futures
        params = {"productType": "USDT-FUTURES", "marginCoin": "USDT"}
        data = await self.request("GET", POSITION_URL, params=params)
        
        if data is None:
            return None

        positions = {} # Key: "SYMBOL-SIDE" (z.B. BTCUSDT-long)
        if data:
            for item in data:
                try:
                    size = float(item.get("total", 0))
                    if size > 0:
                        key = f"{item['symbol']}-{item['holdSide']}"
                        positions[key] = item
                except ValueError:
                    continue
        return positions

    async def get_plan_orders(self):
        """Holt ALLE pending Plan Orders (TP/SL)."""
        # Wir holen 'normal_plan'. Falls du auch Trailing SL nutzt, müsste man das erweitern.
        params = {"productType": "USDT-FUTURES", "planType": "normal_plan"}
        data = await self.request("GET", PLAN_URL, params=params)
        
        orders = []
        if data and "entrustedList" in data:
            orders = data["entrustedList"]
        return orders

    async def cancel_order(self, symbol, order_id):
        payload = {
            "symbol": symbol,
            "productType": "USDT-FUTURES",
            "marginCoin": "USDT",
            "orderIdList": [{"orderId": order_id}]
        }
        logging.info(f"Storniere Order {order_id} für {symbol}...")
        await self.request("POST", CANCEL_PLAN_URL, data=payload)


# --- LOGIK MODULE ---

def is_price_match(price1, price2, tolerance=0.006):
    """Prüft, ob zwei Preise gleich sind (mit Toleranz für Floats)."""
    if price1 is None or price2 is None:
        return False
    
    return abs(float(price1) - float(price2)) < tolerance

async def sync_database_state(session, bitget: BitgetClient):
    """
    Kernlogik:
    1. DB Einträge holen.
    2. Bitget Positionen holen.
    3. Abgleich: DB is_active vs. Realität.
    """
    # 1. Hole alle aktiven Signale aus der DB für diesen User
    db_signals = session.query(TradingSignal).filter(
        TradingSignal.is_active == True,
        TradingSignal.user_key == CURRENT_USER_KEY
    ).all()

    # 2. Hole echte Positionen von Bitget
    bitget_positions = await bitget.get_open_positions()
    
    if bitget_positions is None:
        logging.critical("Konnte Positionen nicht von Bitget laden. Breche Sync ab, um Datenverlust zu vermeiden.")
        return [], None

    if not db_signals:
        logging.info(f"Keine aktiven Trades in der Datenbank. {len(bitget_positions)} Bitget Positionen aktiv.")
        # WICHTIGE KORREKTUR: Gib die echten Positionen zurück, damit cleanup_orphaned_orders sie nicht löscht.
        return [], bitget_positions

    logging.info(f"Prüfe {len(db_signals)} aktive DB-Trades gegen {len(bitget_positions)} offene Bitget-Positionen.")
    
    active_db_signals = [] # Liste der Trades, die wirklich noch aktiv sind

    for signal in db_signals:
        # Key bauen: Bitget nutzt 'long'/'short' lowercase
        signal_side = signal.position_type.lower()
        cleaned_symbol = signal.symbol.split('_')[0]
        key = f"{cleaned_symbol}-{signal_side}"
        logging.debug(f"DB-Symbol ({signal.symbol}) wird mit Bitget-Key '{key}' verglichen.")

        if key not in bitget_positions:
            logging.warning(f"Trade {key} in DB aktiv, aber nicht auf Bitget gefunden. Setze is_active=False.")
            signal.is_active = False
            # Optional: Hier könnte man auch das Schließen-Datum setzen
        else:
            active_db_signals.append(signal)
    
    # DB Änderungen committen (Status-Updates)
    session.commit()
    
    return active_db_signals, bitget_positions

async def check_take_profits(session, active_signals, bitget: BitgetClient):
    """
    Prüft für aktive Trades, ob TPs erreicht wurden.
    Logik: Wenn TP-Order "placed" war, aber jetzt in der API-Liste FEHLT, gilt TP als erreicht.
    """
    if not active_signals:
        return

    # Hole alle pending Orders einmalig
    all_plan_orders = await bitget.get_plan_orders()
    
    if all_plan_orders is None:
        logging.error("Plan Orders konnten nicht von Bitget abgerufen werden. Überspringe TP-Check für diesen Durchlauf.")
        return
    
    # Organisiere Orders für schnelleren Zugriff: Dict[Symbol, List[Order]]
    orders_by_symbol = {}
    for order in all_plan_orders:
        sym = order.get("symbol")
        if sym not in orders_by_symbol:
            orders_by_symbol[sym] = []
        orders_by_symbol[sym].append(order)

    updates_count = 0

    for signal in active_signals:
        
        cleaned_symbol = signal.symbol.split('_')[0]
        symbol_orders = orders_by_symbol.get(cleaned_symbol, [])
        # Helper: Prüft ob eine Order mit diesem Trigger-Preis existiert
        def order_exists_at_price(price):
            if price is None: return False
            logging.debug(f"DEBUG {cleaned_symbol}: Suche Bitget Order mit Preis: {price}")
            for o in symbol_orders:
                trigger = float(o.get("triggerPrice", 0))
                logging.debug(f"DEBUG {cleaned_symbol}: Gefundenen Orderpreis: {trigger}")
                # Wichtig: Wir müssen sicherstellen, dass es eine TP Order ist (Side checken)
                # Long Position -> TP ist Sell Order (side='close_long' oder 'sell')
                # Wir prüfen hier nur den Preis, das ist meist eindeutig genug bei TPs
                if is_price_match(trigger, price):
                    return True
                
            logging.debug(f"DEBUG {cleaned_symbol}: KEIN MATCH für Preis {price}")
            return False
        
        # --- Allgemeine Korrektur-Funktion ---
        def run_tp_checks(tp_index, price, placed_flag, reached_flag):
            nonlocal updates_count # Erlaubt die Änderung von updates_count
            
            order_exists = order_exists_at_price(price) # Einmal prüfen
            
            # TEIL 1: Korrektur von False -> True (Order existiert, aber DB vergisst es)
            if order_exists_at_price(price) and not placed_flag:
                logging.warning(f"DB-Korrektur: Order für TP{tp_index} {signal.symbol} auf Bitget gefunden, aber 'placed' war False. Setze auf True.")
                placed_flag = True
                updates_count += 1

            # TEIL 2: Korrektur von True -> False (Order fehlt, aber DB glaubt, sie wurde platziert, aber nicht erreicht)
            # DIES IST DIE KRITISCHE KORREKTUR für Ihr Lawinen-Problem!
            elif placed_flag and not order_exists_at_price(price) and not reached_flag:
                logging.warning(f"DB-Korrektur: Order für TP{tp_index} {signal.symbol} fehlt auf Bitget, aber 'placed' war True. Setze auf False, da TP nicht erreicht wurde.")
                placed_flag = False
                updates_count += 1
            
            # TEIL 3: Echte TP-Erreichung (Wird nur geprüft, wenn placed=True und nicht reached=True)
            if placed_flag and not reached_flag:
                if not order_exists_at_price(price):
                    logging.info(f"TP{tp_index} für {signal.symbol} erreicht (Order nicht mehr gefunden).")
                    reached_flag = True
                    updates_count += 1
            
            return placed_flag, reached_flag

        # --- Ausführen der Checks ---
        signal.tp1_order_placed, signal.tp1_reached = run_tp_checks(
            1, signal.tp1_price, signal.tp1_order_placed, signal.tp1_reached
        )
        
        signal.tp2_order_placed, signal.tp2_reached = run_tp_checks(
            2, signal.tp2_price, signal.tp2_order_placed, signal.tp2_reached
        )

        signal.tp3_order_placed, signal.tp3_reached = run_tp_checks(
            3, signal.tp3_price, signal.tp3_order_placed, signal.tp3_reached
        )
        # --- Ende der Checks ---

    if updates_count > 0:
        session.commit()
        logging.info(f"{updates_count} TP-Status Updates in DB gespeichert.")

async def cleanup_orphaned_orders(bitget_positions, bitget: BitgetClient):
    """
    Löscht Orders, die keine offene Position mehr haben.
    Filtert auf 'close' Orders, um Entry-Orders nicht zu löschen.
    """
    all_orders = await bitget.get_plan_orders()
    
    if all_orders is None:
        logging.error("Konnte Plan Orders nicht von Bitget abrufen. Breche Order-Cleanup ab.")
        return

    # Set mit Strings "SYMBOL-SIDE" der offenen Positionen
    active_position_keys = set(bitget_positions.keys())
    
    orphans_found = 0
    
    for order in all_orders:
        symbol = order.get("symbol")
        pos_side = order.get("posSide") # long / short
        trade_side = order.get("tradeSide") # open / close
        
        # Sicherheitscheck: Manchmal ist posSide leer bei Net-Mode, aber hier nehmen wir an Hedge-Mode
        if not pos_side or not trade_side:
            continue
            
        # 1. Wir löschen NUR Closing Orders (SL/TP).
        # Open Orders sind Limit-Entries für neue Trades -> Finger weg!
        if trade_side.lower() != "close":
            continue
            
        # 2. Key bauen und prüfen
        # Format in active_position_keys ist "BTCUSDT-long"
        key = f"{symbol}-{pos_side}"
        
        if key not in active_position_keys:
            logging.warning(f"Verwaiste Order gefunden: {symbol} {pos_side} (ID: {order['orderId']}). Keine aktive Position. Lösche...")
            await bitget.cancel_order(symbol, order["orderId"])
            orphans_found += 1
            # Kleines Sleep um API Limit nicht zu sprengen
            await asyncio.sleep(0.1)

    if orphans_found > 0:
        logging.info(f"Bereinigung abgeschlossen: {orphans_found} verwaiste Orders gelöscht.")
    else:
        logging.info("Keine verwaisten Orders gefunden.")

# --- MAIN LOOP ---

async def main():
    logging.info("--- Position Handler Start ---")
    
    bitget = BitgetClient()
    session = SessionLocal()
    
    try:
        # Schritt 1: DB Sync & Positionen holen
        active_signals, bitget_positions = await sync_database_state(session, bitget)
        
        if bitget_positions is None:
            logging.error("SICHERHEITSSTOPP: Da Positionsdaten fehlen, werden keine TPs geprüft und keine Orders gelöscht.")
            return

        # Schritt 2: TPs prüfen (nur wenn Trades aktiv sind)
        if active_signals:
            await check_take_profits(session, active_signals, bitget)
        
        # Schritt 3: Aufräumen (immer prüfen, auch wenn keine DB Trades aktiv sind)
        # Wir übergeben bitget_positions, damit wir nicht nochmal API callen müssen
        await cleanup_orphaned_orders(bitget_positions, bitget)

    except Exception as e:
        logging.critical(f"Unbehandelter Fehler im Main Loop: {e}", exc_info=True)
    finally:
        session.close()
        await bitget.close()
        logging.info("--- Position Handler Ende ---")

if __name__ == "__main__":
    asyncio.run(main())