import asyncio
import hashlib
import hmac
import time
import json
import os
import sys
import logging
import base64
from datetime import datetime
import httpx
from dotenv import load_dotenv

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

if not all([API_KEY, SECRET_KEY, PASSPHRASE]):
    logging.critical("Fehlende API-Umgebungsvariablen. Beende Programm.")
    sys.exit(1)

BASE_URL = "https://api.bitget.com"

# --- BITGET CLIENT ---
class BitgetClient:
    def __init__(self):
        self.api_key = API_KEY
        self.secret_key = SECRET_KEY
        self.passphrase = PASSPHRASE

    def generate_signature(self, timestamp, method, request_path, body=""):
        message = str(timestamp) + method.upper() + request_path + body
        sig_bin = hmac.new(
            self.secret_key.encode('utf-8'),
            message.encode('utf-8'),
            hashlib.sha256
        ).digest()
        return base64.b64encode(sig_bin).decode('utf-8')

    async def request(self, method, path, params=None, data=None):
        timestamp = str(int(time.time() * 1000))
        method = method.upper()
        
        # 1. Query String sauber aufbauen
        query_string = ""
        if params:
            # WICHTIG: Keine Leerzeichen, exakte Key-Value Paare
            query_string = "&".join([f"{k}={v}" for k, v in params.items()])
        
        # 2. Den Pfad für die Signatur und URL festlegen
        # Bei GET-Requests MÜSSEN die Parameter in den sign_path
        sign_path = path
        if method == "GET" and query_string:
            sign_path = f"{path}?{query_string}"
        
        # Die vollständige URL für den Request
        full_url = f"{BASE_URL}{sign_path}"
        
        # 3. Body für POST vorbereiten
        body = json.dumps(data) if data else ""
        
        # 4. Signatur generieren
        signature = self.generate_signature(timestamp, method, sign_path, body)

        headers = {
            "ACCESS-KEY": self.api_key,
            "ACCESS-SIGN": signature,
            "ACCESS-PASSPHRASE": self.passphrase,
            "ACCESS-TIMESTAMP": timestamp,
            "Content-Type": "application/json"
        }

        async with httpx.AsyncClient(timeout=10) as client:
            try:
                if method == "GET":
                    # WICHTIG: Wir übergeben params=None, da sie bereits in full_url stecken!
                    resp = await client.get(full_url, headers=headers)
                else:
                    resp = await client.post(full_url, headers=headers, data=body)
                
                return resp.json()
            except Exception as e:
                logging.error(f"API Request Error: {e}")
                return None
        
# --- LOGIK: VERWAISTE ORDERS LÖSCHEN ---

async def cleanup_orphaned_orders(bitget_positions, bitget):
    """Löscht Plan-Orders (TP/SL), für die keine offene Position mehr existiert."""
    logging.info("Prüfe auf verwaiste Plan-Orders...")
    
    # Holen aller offenen Plan-Orders
    pending_resp = await bitget.request("GET", "/api/v2/mix/order/orders-plan-pending",
        params={
            "planType": "normal_plan",
            "productType": "USDT-FUTURES"
        }
    )

    if not pending_resp or pending_resp.get("code") != "00000":
        logging.error("Konnte offene Plan-Orders nicht abrufen.")
        return

    open_plan_orders = pending_resp.get("data", {}).get("entrustedList", [])
    if not open_plan_orders:
        logging.info("Keine offenen Plan-Orders gefunden.")
        return

    active_symbols = set()
    for pos in bitget_positions:
        raw_symbol = pos.get('symbol', '').upper()
        clean_symbol = raw_symbol.split('_')[0]
        if abs(float(pos.get('total', 0))) > 0:
            active_symbols.add(clean_symbol)

    orphans_found = 0
    for order in open_plan_orders:
        symbol = order.get('symbol')
        # Wenn eine Order für ein Symbol existiert, in dem wir nicht investiert sind -> Löschen
        if symbol not in active_symbols:
            logging.warning(f"Verwaiste Order gefunden: {order.get('orderId')} für {symbol}. Lösche...")
            
            cancel_payload = {
                "symbol": symbol,
                "productType": "UMCBL",
                "orderId": order.get('orderId')
            }
            del_resp = await bitget.request("POST", "/api/v2/mix/order/cancel-plan-order", data=cancel_payload)
            
            if del_resp and del_resp.get("code") == "00000":
                orphans_found += 1
                logging.info(f"Erfolgreich gelöscht: {order.get('orderId')}")
            else:
                logging.error(f"Fehler beim Löschen von {order.get('orderId')}: {del_resp}")

    logging.info(f"Bereinigung abgeschlossen: {orphans_found} verwaiste Orders gelöscht.")

# --- MAIN LOOP ---

async def main():
    logging.info("--- Position Handler (Cleanup Mode) Start ---")
    bitget = BitgetClient()
    
    try:
        # Nur Positionen abrufen
        pos_resp = await bitget.request("GET", "/api/v2/mix/position/all-position", {"productType": "USDT-FUTURES"})
        
        if pos_resp and pos_resp.get("code") == "00000":
            bitget_positions = pos_resp.get("data", [])
            # Verwaiste Orders bereinigen
            await cleanup_orphaned_orders(bitget_positions, bitget)
        else:
            logging.error(f"Konnte Positionen nicht abrufen: {pos_resp}")

    except Exception as e:
        logging.critical(f"Fehler im Main Loop: {e}")

if __name__ == "__main__":
    asyncio.run(main())