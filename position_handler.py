import asyncio
import hashlib
import hmac
import time
import json
import os
import httpx
import logging
import sys 
import base64 
from dotenv import load_dotenv

# --- Konfiguration und Konstanten ---
# Loggt standardmäßig INFO, WARNING, ERROR, CRITICAL. Debug-Meldungen werden unterdrückt.
# Wenn Sie weniger sehen möchten, setzen Sie es auf logging.WARNING
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', stream=sys.stdout) 

# Bitget API Endpunkte
BASE_URL = "https://api.bitget.com"
PLAN_URL = "/api/mix/v1/plan/currentPlan"
POSITION_URL = "/api/mix/v1/position/allPosition" 
CANCEL_PLAN_URL = "/api/mix/v1/plan/cancelPlan" 
PLACE_PLAN_URL = "/api/mix/v1/plan/placePlan"
TIME_URL = "/api/v2/public/time" 

# --- Globale Variablen ---
TIME_OFFSET_MS = 0 

# --- Umgebungsvariablen laden ---
load_dotenv() 

API_KEY = os.getenv("BITGET_API_KEY")
SECRET_KEY = os.getenv("BITGET_API_SECRET")
PASSPHRASE = os.getenv("BITGET_PASSWORD")

if not all([API_KEY, SECRET_KEY, PASSPHRASE]):
    print("FATAL ERROR: Umgebungsvariablen (BITGET_API_KEY, BITGET_API_SECRET, BITGET_PASSWORD) fehlen. Programm wird beendet.")
    # Reduziert: Fehlerlog bleibt kritisch
    logging.critical("Umgebungsvariablen fehlen. Beende Programm.") 
    sys.exit(1)

# Reduziert: Erfolgsmeldung bleibt
logging.info("Umgebungsvariablen erfolgreich geladen und geprüft.") 

# --- Hilfsfunktionen für die API-Kommunikation ---

def generate_bitget_signature(timestamp: str, method: str, request_path: str, body: str = "") -> str:
    """Generiert die Bitget API-Signatur (Base64-kodiert)."""
    message = timestamp + method.upper() + request_path + body
    hmac_key = SECRET_KEY.encode('utf-8')
    h = hmac.new(hmac_key, message.encode('utf-8'), hashlib.sha256)
    signature = base64.b64encode(h.digest()).decode()
    return signature

def get_bitget_timestamp() -> str:
    """Gibt den korrigierten, synchronisierten Bitget-Timestamp zurück."""
    global TIME_OFFSET_MS
    return str(int(time.time() * 1000) + TIME_OFFSET_MS)

def get_headers(method: str, path: str, body: dict = None) -> dict:
    """Erstellt die notwendigen HTTP-Header für Bitget."""
    timestamp = get_bitget_timestamp() 
    body_str = json.dumps(body, separators=(',', ':')) if body else "" 
    signature = generate_bitget_signature(timestamp, method, path, body_str)

    return {
        "Content-Type": "application/json",
        "ACCESS-KEY": API_KEY,
        "ACCESS-SIGN": signature,
        "ACCESS-TIMESTAMP": timestamp,
        "ACCESS-PASSPHRASE": PASSPHRASE,
        "locale": "en-US"
    }

async def make_api_request(client: httpx.AsyncClient, method: str, url: str, params: dict = None, json_data: dict = None):
    """Führt einen API-Request aus und verarbeitet Fehler (nutzt übergebenen Client)."""
    method = method.upper()
    path = url.replace(BASE_URL, '')
    
    signed_path = path
    if method == "GET" and params:
        query_string = str(httpx.QueryParams(params)) 
        if query_string:
            signed_path += "?" + query_string
            
    headers = get_headers(method, signed_path, json_data)

    try:
        if method == "GET":
            response = await client.get(path, headers=headers, params=params) 
        else: # POST
            response = await client.post(path, headers=headers, json=json_data)
        
        response.raise_for_status()
        data = response.json()
        
        if data.get("code") != "00000":
            # Tolerierte Fehler bleiben als WARNING
            if data.get("code") in ["40034", "43020"]:
                logging.warning(f"Bitget API Fehler (Code: {data.get('code')}): Order existiert nicht. IGNORIERE.")
                return {} 
                
            # Bei anderen Fehlern: Nur kritische Daten loggen
            logging.error(f"Bitget API Fehler (Code: {data.get('code')}): {data.get('msg')} für {path}")
            return None
        
        return data.get("data")
    
    except httpx.HTTPStatusError as e:
        # HTTP Status Fehler bleiben detailliert, da sie oft schwer zu debuggen sind
        logging.error(f"HTTP-Statusfehler beim Aufruf von {path}: {e.response.status_code}")
        
        try:
            error_data = e.response.json()
            error_code = error_data.get("code")
            
            if e.response.status_code == 400 and error_code in ["43020", "40034"]:
                logging.warning(f"  -> Bitget API Fehler (Code: {error_code}): Order existiert nicht (im HTTP-Fehlerblock abgefangen). IGNORIERE.")
                return {} 

        except Exception:
            pass 

        try:
            response_text = e.response.text
        except Exception:
            response_text = "Kein Response-Text verfügbar."
        
        # Details auf Debug Level reduzieren
        logging.debug(f"-> Response Body (Text): {response_text}")
        logging.debug(f"-> Response Header: {dict(e.response.headers)}")
        return None
    
    except httpx.RequestError as e:
        # Netzwerkfehler bleiben wichtig
        logging.error(f"Netzwerk- oder Request-Fehler beim Aufruf von {path}: {e}")
        return None


async def sync_server_time():
    """Ruft die Bitget Serverzeit ab und berechnet den Zeitversatz."""
    global TIME_OFFSET_MS
    
    logging.info("Synchronisiere Zeit mit Bitget Server...")
    
    local_timestamp_start = int(time.time() * 1000)
    
    async with httpx.AsyncClient(base_url=BASE_URL, timeout=5.0) as client:
        try:
            response = await client.get(TIME_URL)
            response.raise_for_status()
            data = response.json()
            
            server_timestamp_raw = data.get("data", {}).get("serverTime")
            server_timestamp = 0
            
            if server_timestamp_raw:
                try:
                    server_timestamp = int(server_timestamp_raw)
                except (ValueError, TypeError):
                    # Reduziert: Fehlerlog bleibt
                    logging.error(f"Konvertierungsfehler: Konnte Server-Timestamp ({server_timestamp_raw}) nicht in Integer umwandeln.")
                    server_timestamp = 0
            
            local_timestamp_end = int(time.time() * 1000)
            
            if server_timestamp:
                TIME_OFFSET_MS = server_timestamp - local_timestamp_end
                logging.info(f"Zeit-Synchronisation erfolgreich. Lokale Abweichung: {TIME_OFFSET_MS} ms.")
            else:
                logging.warning(f"Konnte Server-Timestamp nicht korrekt abrufen. Verwende lokale Zeit.")
        
        except Exception as e:
            logging.error(f"Fehler bei der Zeit-Synchronisation: {e}. Verwende lokale Zeit.")


# --- Kernlogik des Handlers ---

async def get_all_open_positions(client: httpx.AsyncClient):
    """Ruft alle offenen Positionen ab und gibt ein Dictionary zurück: {symbol: size}."""
    logging.info("Schritt 1: Rufe alle offenen Positionen ab.")
    
    params = {"productType": "UMCBL"}
    positions_data = await make_api_request(client, "GET", BASE_URL + POSITION_URL, params=params)
    
    open_positions = {}
    position_list_from_api = positions_data if isinstance(positions_data, list) else [] 

    if position_list_from_api:
        for pos in position_list_from_api:
            total_size_raw = pos.get("total", "0")
            
            try:
                hold_size = float(total_size_raw)
            except (ValueError, TypeError):
                # Reduziert: Fehlerlog bleibt
                logging.error(f"Fehler beim Konvertieren der Positionsgröße 'total' für {pos.get('symbol')}.")
                continue

            if hold_size > 0:
                symbol = pos["symbol"]
                entry_price_raw = pos.get("averageOpenPrice", pos.get("averageOpenPriceUsd"))
                try:
                    entry_price = float(entry_price_raw)
                except (ValueError, TypeError):
                    entry_price = None 
                    
                open_positions[symbol] = {
                    "size": hold_size, 
                    "side": pos["holdSide"],
                    "entry_price": entry_price
                }
                # Reduziert: Wichtige Positionen bleiben INFO
                logging.info(f"-> Position gefunden: {symbol}, Größe: {hold_size:.4f}, Seite: {pos['holdSide']}")
            
            # Reduziert: Null-Positionen auf DEBUG
            elif hold_size == 0 and pos.get("symbol"):
                logging.debug(f"-> Ignoriere Null-Position/Hedge-Side: {pos['symbol']}")

        if not open_positions:
            # Log bleibt wichtig
            logging.warning("API-Aufruf lieferte Daten, aber keine aktive Position (> 0 Größe) gefunden.")
            # Entfernt: Die langen Warnungen über Kontotypen etc. (Einmaliger Check genügt)
    
    elif positions_data is None:
        logging.error("API-Anfrage für Positionen ist fehlgeschlagen.")
    else: 
        logging.info("API-Aufruf für Positionen lieferte eine leere Liste.")

    return open_positions

async def get_sl_and_tp_orders(client: httpx.AsyncClient):
    """Ruft alle ausstehenden Plan-Orders ab und identifiziert SL- und TP-Orders (gefiltert auf Market-Orders)."""
    logging.info("Schritt 2: Rufe alle ausstehenden Conditional Orders ab.")
    
    params = {"productType": "UMCBL"}
    plan_orders_raw = await make_api_request(client, "GET", BASE_URL + PLAN_URL, params=params)
    
    sl_orders = {}
    plan_orders_list = []

    # Straffung der Logik zur Extraktion der Order-Liste
    if isinstance(plan_orders_raw, list):
        plan_orders_list = plan_orders_raw
    elif isinstance(plan_orders_raw, dict):
        if plan_orders_raw.get("planList"):
            plan_orders_list = plan_orders_raw["planList"]
        elif plan_orders_raw.get("orderList"):
            plan_orders_list = plan_orders_raw["orderList"]
        # Reduziert: Das Loggen der erfolgreichen Extraktion entfällt
    elif plan_orders_raw is None:
        logging.warning("Keine Conditional Order Daten von der API erhalten.")
        return sl_orders
    
    if plan_orders_list:
        # Reduziert: Die Order-Objekt-Prüfung wurde gestrafft.
        for order in plan_orders_list:
            
            if not all(k in order for k in ["symbol", "orderId", "size", "triggerPrice"]):
                logging.warning(f"Order-Objekt unvollständig. Überspringe.")
                continue

            symbol = order["symbol"]
            
            if order.get("orderType") == "market": 
                sl_orders[symbol] = {
                    "planId": order["orderId"],
                    "size": float(order["size"]),
                    "triggerPrice": float(order["triggerPrice"])
                }
                # Reduziert: Wichtige Order-Infos bleiben INFO
                logging.info(f"-> SL-Order gefunden: {symbol}, Plan-ID: {order['orderId']}, Größe: {order['size']}")
            else:
                # Reduziert: Ignorierte Orders auf DEBUG
                logging.debug(f"-> Ignoriere Nicht-Market Plan Order: {symbol}, Type: {order.get('orderType')}")

    return sl_orders

async def cancel_and_replace_sl(client: httpx.AsyncClient, symbol: str, old_sl: dict, new_size: float, position_side: str, entry_price: float | None):
    """Storniert die alte SL-Order und platziert eine neue mit korrigierter Größe."""
    
    old_plan_id = old_sl["planId"]
    old_size = old_sl["size"]
    trigger_price = old_sl["triggerPrice"]
    rounded_trigger_price = round(trigger_price, 4)

    # Korrektur-Logs bleiben als WARNING / INFO (wichtige Vorgänge)
    logging.warning(f"--- KORREKTUR ERFORDERLICH für {symbol} ---")
    logging.warning(f"  Position: {new_size:.4f}, SL-Order: {old_size:.4f} (Plan-ID: {old_plan_id})")
    
    # 1. Storniere die alte, überdimensionierte SL-Order
    cancel_payload = {
        "symbol": symbol,
        "productType": "UMCBL",
        "marginCoin": "USDT", 
        "orderId": old_plan_id, 
        "planType": "normal_plan" 
    }
    logging.info(f"  -> Storniere alte SL-Order {old_plan_id}...")
    
    cancel_result = await make_api_request(client, "POST", BASE_URL + CANCEL_PLAN_URL, json_data=cancel_payload)

    if cancel_result is None:
        logging.error(f"  !! Fehler beim Stornieren der SL-Order {old_plan_id}. Abbruch der Korrektur.")
        return
    elif cancel_result == {}:
        logging.warning(f"  -> Stornierungsversuch fehlgeschlagen (Order existierte nicht). Platziere dennoch neue SL-Order.")
    else:
        logging.info(f"  -> Stornierung erfolgreich. Platziere neue SL-Order.")


    # 2. Platziere die neue SL-Order mit der korrekten Größe (new_size)
    
    if position_side == "long":
        new_side = "close_long"
    elif position_side == "short":
        new_side = "close_short"
    else:
        logging.error(f"  !! Fehler: Unbekannte Position Side ({position_side}). Abbruch der Platzierung.")
        return
    
    execute_price = "0"
    
    place_payload = {
        "symbol": symbol,
        "size": str(round(new_size, 4)), 
        "side": new_side, 
        "orderType": "market", 
        "productType": "UMCBL",
        "marginCoin": "USDT",
        "triggerPrice": str(rounded_trigger_price),
        "triggerType": "mark_price",
        "executePrice": execute_price 
    }
    
    logging.info(f"  -> Platziere neue SL-Order: Side={new_side}, Größe={new_size:.4f}, Trigger={rounded_trigger_price}")
    new_order_result = await make_api_request(client, "POST", BASE_URL + PLACE_PLAN_URL, json_data=place_payload)
    
    if new_order_result:
        logging.info(f"  -> Korrektur erfolgreich! Neue Plan-ID: {new_order_result.get('orderId')}")
    else:
        logging.error("  !! Kritischer Fehler: Neue SL-Order konnte nicht platziert werden.")


async def run_sl_correction_check(client: httpx.AsyncClient):
    """Die Hauptfunktion, die alle Schritte des Polling-Prozesses durchläuft."""
    
    open_positions = await get_all_open_positions(client)
    
    if not open_positions:
        logging.info("Keine offenen Positionen gefunden. Beende Check.")
        return

    sl_orders = await get_sl_and_tp_orders(client)

    corrected_count = 0
    missing_sl_symbols = [] 
    
    for symbol, pos_data in open_positions.items():
        current_size = pos_data["size"]
        position_side = pos_data["side"]
        entry_price = pos_data["entry_price"] 
        
        if symbol in sl_orders:
            sl_data = sl_orders[symbol]
            registered_sl_size = sl_data["size"]
            
            if abs(current_size - registered_sl_size) > 0.0001:
                logging.info(f"*** ABWEICHUNG ERKANNT für {symbol} ***")
                # Reduziert: Nur die Fakten loggen
                logging.info(f"  - Offene Größe: {current_size:.4f}, SL-Größe: {registered_sl_size:.4f}")
                
                await cancel_and_replace_sl(
                    client=client, 
                    symbol=symbol, 
                    old_sl=sl_data, 
                    new_size=current_size, 
                    position_side=position_side,
                    entry_price=entry_price
                )
                corrected_count += 1
            else:
                # Reduziert: Synchrone Positionen auf DEBUG
                logging.debug(f"Position für {symbol} ist synchronisiert. Größe: {current_size:.4f}")
        else:
            missing_sl_symbols.append(symbol)
            logging.warning(f"Offene Position für {symbol} hat KEINE aktive SL-Order.")

    if missing_sl_symbols:
        logging.warning(f"ZUSAMMENFASSUNG: Für die folgenden Symbole fehlen aktive Stop-Loss Orders: {', '.join(missing_sl_symbols)}.")

    # Reduziert: Erfolgsmeldung bleibt
    logging.info(f"Polling-Durchlauf abgeschlossen. Korrigierte SL-Orders: {corrected_count}.")


async def main_loop():
    """Führt einen einzelnen Durchlauf des Position Handlers aus (für Cron-Scheduler)."""
    await sync_server_time()
    
    async with httpx.AsyncClient(base_url=BASE_URL, timeout=10.0) as client:
        logging.info("--- Bitget Position Handler gestartet ---")
        try:
            await run_sl_correction_check(client) 
        except Exception as e:
            logging.critical(f"Kritischer Fehler im Haupt-Loop: {e}")
        
    logging.info("--- Bitget Position Handler beendet ---")

if __name__ == "__main__":
    try:
        asyncio.run(main_loop())
    except KeyboardInterrupt:
        logging.info("Programm durch Benutzer beendet.")
    except Exception as e:
        logging.critical(f"Unerwarteter Fehler im Main: {e}")