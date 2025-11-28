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

# Setzt das Logging-Level auf INFO: Wichtige Schritte, Warnungen und Fehler werden angezeigt.
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', stream=sys.stdout) 

# Bitget API Endpunkte
BASE_URL = "https://api.bitget.com"
PLAN_URL = "/api/mix/v1/plan/currentPlan"
POSITION_URL = "/api/mix/v1/position/allPosition" 
CANCEL_PLAN_URL = "/api/mix/v1/plan/cancelPlan" 
PLACE_PLAN_URL = "/api/mix/v1/plan/placePlan"
TIME_URL = "/api/v2/public/time" 

# --- Globale Variablen und Initialisierung ---

TIME_OFFSET_MS = 0 

# Umgebungsvariablen laden
load_dotenv() 

API_KEY = os.getenv("BITGET_API_KEY")
SECRET_KEY = os.getenv("BITGET_API_SECRET")
PASSPHRASE = os.getenv("BITGET_PASSWORD")

if not all([API_KEY, SECRET_KEY, PASSPHRASE]):
    print("FATAL ERROR: Umgebungsvariablen (BITGET_API_KEY, BITGET_API_SECRET, BITGET_PASSWORD) fehlen. Programm wird beendet.")
    logging.critical("Umgebungsvariablen fehlen. Beende Programm.") 
    sys.exit(1)

logging.info("Umgebungsvariablen erfolgreich geladen und geprüft.") 

# --- Hilfsfunktionen für die API-Kommunikation ---

def generate_bitget_signature(timestamp: str, method: str, request_path: str, body: str = "") -> str:
    """Generiert die Bitget API-Signatur (Base64-kodiert) basierend auf den Request-Details."""
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
    """Erstellt die notwendigen HTTP-Header für Bitget, inklusive Signatur."""
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
    """Führt einen API-Request aus und verarbeitet gängige Bitget-Fehler."""
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
            # Tolerierte Fehler (z.B. Order existiert nicht mehr)
            if data.get("code") in ["40034", "43020"]:
                logging.warning(f"Bitget API Fehler (Code: {data.get('code')}): Order existiert nicht. IGNORIERE.")
                return {} 
                
            logging.error(f"Bitget API Fehler (Code: {data.get('code')}): {data.get('msg')} für {path}")
            return None
        
        return data.get("data")
    
    except httpx.HTTPStatusError as e:
        logging.error(f"HTTP-Statusfehler beim Aufruf von {path}: {e.response.status_code}")
        
        try:
            error_data = e.response.json()
            error_code = error_data.get("code")
            
            if e.response.status_code == 400 and error_code in ["43020", "40034"]:
                logging.warning(f"  -> Bitget API Fehler (Code: {error_code}): Order existiert nicht (im HTTP-Fehlerblock abgefangen). IGNORIERE.")
                return {} 

        except Exception:
            pass 

        return None
    
    except httpx.RequestError as e:
        logging.error(f"Netzwerk- oder Request-Fehler beim Aufruf von {path}: {e}")
        return None


async def sync_server_time():
    """Ruft die Bitget Serverzeit ab und berechnet den Zeitversatz zur lokalen Uhr."""
    global TIME_OFFSET_MS
    
    logging.info("Synchronisiere Zeit mit Bitget Server...")
    
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
    """Ruft alle offenen Positionen ab und gibt ein Dictionary zurück: {symbol: position_data}."""
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
                logging.error(f"Fehler beim Konvertieren der Positionsgröße 'total' für {pos.get('symbol')}.")
                continue

            if hold_size > 0:
                symbol = pos["symbol"]
                
                # Extrahieren des angehängten SL/TP Preises
                sl_price = float(pos.get("stopLossPrice")) if pos.get("stopLossPrice") else None
                tp_price = float(pos.get("takeProfitPrice")) if pos.get("takeProfitPrice") else None

                entry_price = None
                
                entry_price_raw = pos.get("averageOpenPrice") or pos.get("averageOpenPriceUsd")

                if entry_price_raw:
                    try:
                        entry_price = float(entry_price_raw)
                    except (ValueError, TypeError):
                        logging.error(f"FEHLER: Konnte Einstiegspreis '{entry_price_raw}' für {symbol} nicht in Float umwandeln.")
                
                if entry_price is None or entry_price <= 0:
                     logging.warning(f"WARNUNG: Einstiegspreis (Entry Price) für {symbol} konnte nicht korrekt abgerufen werden oder ist 0/None. Conditional SL/TP Check wird übersprungen.")
                     # Hier setzen wir entry_price absichtlich auf None, aber loggen die Warnung
                    
                open_positions[symbol] = {
                    "size": hold_size, 
                    "side": pos["holdSide"],
                    "entry_price": entry_price,
                    "stop_loss_price": sl_price,
                    "take_profit_price": tp_price
                }
                logging.info(f"-> Position gefunden: {symbol}, Größe: {hold_size:.4f}, Seite: {pos['holdSide']}")
            
            elif hold_size == 0 and pos.get("symbol"):
                logging.debug(f"-> Ignoriere Null-Position/Hedge-Side: {pos['symbol']}")

        if not open_positions:
            logging.warning("API-Aufruf lieferte Daten, aber keine aktive Position (> 0 Größe) gefunden.")
    
    elif positions_data is None:
        logging.error("API-Anfrage für Positionen ist fehlgeschlagen.")
    else: 
        logging.info("API-Aufruf für Positionen lieferte eine leere Liste.")

    return open_positions

async def get_sl_and_tp_orders(client: httpx.AsyncClient):
    """
    Ruft alle ausstehenden Plan-Orders ab und sammelt diese. 
    Wichtig: Sammelt ALLE Plan-Orders, unabhängig vom Typ, um sie auf 'verwaist' prüfen zu können.
    """
    logging.info("Schritt 2: Rufe alle ausstehenden Conditional Orders ab.")
    
    params = {"productType": "UMCBL"}
    plan_orders_raw = await make_api_request(client, "GET", BASE_URL + PLAN_URL, params=params)
    
    # all_plan_orders speichert eine LISTE von Conditional Orders pro Symbol
    all_plan_orders = {} 
    plan_orders_list = []

    if isinstance(plan_orders_raw, list):
        plan_orders_list = plan_orders_raw
    elif isinstance(plan_orders_raw, dict):
        if plan_orders_raw.get("planList"):
            plan_orders_list = plan_orders_raw["planList"]
        elif plan_orders_raw.get("orderList"):
            plan_orders_list = plan_orders_raw["orderList"]
    elif plan_orders_raw is None:
        logging.warning("Keine Conditional Order Daten von der API erhalten.")
        return all_plan_orders # Korrekte Rückgabe

    if plan_orders_list:
        for order in plan_orders_list:
            
            if not all(k in order for k in ["symbol", "orderId", "size", "triggerPrice"]):
                logging.warning(f"Order-Objekt unvollständig. Überspringe.")
                continue

            symbol = order["symbol"]
            order_side = order.get("side")

            if order_side not in ["close_long", "close_short"]:
                 logging.debug(f"Ignoriere Plan Order {order['orderId']} für {symbol}: Nicht-Schließungs-Side ({order_side}).")
                 continue
            
            # --- KORRIGIERTE LOGIK: Sammelt ALLE Plan Orders zur Stornierung ---
            # Der vorherige Filter (orderType == "market") wurde entfernt.
            
            try:
                order_data = {
                    "planId": order["orderId"],
                    "size": float(order["size"]),
                    "triggerPrice": float(order["triggerPrice"]),
                    "side": order["side"],
                    "orderType": order.get("orderType") # Typ für Debugging/Logging
                }
            except (ValueError, TypeError) as e:
                 logging.error(f"Fehler bei Konvertierung in Order-Data für {symbol}: {e}. Überspringe.")
                 continue
            
            if symbol not in all_plan_orders:
                all_plan_orders[symbol] = []
            
            # --- KORREKTUR der fehlerhaften Listenzuweisung/append:
            all_plan_orders[symbol].append(order_data) 
            
            logging.info(f"-> Conditional Order gefunden: {symbol}, Plan-ID: {order['orderId']}, Größe: {order_data['size']}, Trigger: {order_data['triggerPrice']}, Typ: {order_data['orderType']}")

    return all_plan_orders

async def cancel_conditional_order(client: httpx.AsyncClient, symbol: str, plan_id: str):
    """Storniert eine Conditional Order anhand ihrer Plan-ID."""
    cancel_payload = {
        "symbol": symbol,
        "productType": "UMCBL",
        "marginCoin": "USDT", 
        "orderId": plan_id, 
        "planType": "normal_plan" 
    }
    # Rückgabe des Ergebnisses ist hier optional, da wir nur die Ausführung benötigen
    return await make_api_request(client, "POST", BASE_URL + CANCEL_PLAN_URL, json_data=cancel_payload)


async def cancel_and_replace_sl(client: httpx.AsyncClient, symbol: str, old_sl: dict, new_size: float, position_side: str, entry_price: float | None):
    """Storniert die alte SL-Order und platziert eine neue mit korrigierter, voller Positionsgröße."""
    
    old_plan_id = old_sl["planId"]
    old_size = old_sl["size"]
    trigger_price = old_sl["triggerPrice"]
    rounded_trigger_price = round(trigger_price, 4)

    logging.warning(f"--- KORREKTUR ERFORDERLICH für {symbol} ---")
    logging.warning(f"  Position: {new_size:.4f}, SL-Order: {old_size:.4f} (Plan-ID: {old_plan_id})")
    
    # 1. Storniere die alte, überdimensionierte SL-Order
    logging.info(f"  -> Storniere alte SL-Order {old_plan_id}...")
    # cancel_result wird nur benötigt, um den nächsten Schritt zu bedingen
    cancel_result = await cancel_conditional_order(client, symbol, old_plan_id) 

    if cancel_result is None:
        logging.error(f"  !! Fehler beim Stornieren der SL-Order {old_plan_id}. Abbruch der Platzierung.")
        return
    
    if cancel_result == {}:
        logging.warning(f"  -> Stornierungsversuch fehlgeschlagen (Order existierte nicht oder tolerierbarer API-Fehler). Platziere dennoch neue SL-Order.")
    else:
        # Stornierung war erfolgreich (Code 00000)
        logging.info(f"  -> Stornierung erfolgreich. Platziere neue SL-Order.")

    # 2. Platziere die neue SL-Order mit der korrekten Größe (new_size)
    
    if position_side == "long":
        new_side = "close_long"
    elif position_side == "short":
        new_side = "close_short"
    else:
        logging.error(f"  !! Fehler: Unbekannte Position Side ({position_side}). Abbruch der Platzierung.")
        return
    
    limit_price = str(rounded_trigger_price)
    
    place_payload = {
        "symbol": symbol,
        "size": str(round(new_size, 4)), 
        "side": new_side, 
        "orderType": "limit", 
        "productType": "UMCBL",
        "marginCoin": "USDT",
        "triggerPrice": str(rounded_trigger_price),
        "triggerType": "mark_price",
        "executePrice": limit_price 
    }
    
    logging.info(f"  -> Platziere neue SL-Order: Side={new_side}, Größe={new_size:.4f}, Trigger={rounded_trigger_price}")
    new_order_result = await make_api_request(client, "POST", BASE_URL + PLACE_PLAN_URL, json_data=place_payload)
    
    if new_order_result:
        logging.info(f"  -> Korrektur erfolgreich! Neue Plan-ID: {new_order_result.get('orderId')}")
    else:
        logging.error("  !! Kritischer Fehler: Neue SL-Order konnte nicht platziert werden.")

async def cancel_orphan_plan_orders(client: httpx.AsyncClient, open_positions: dict, all_plan_orders: dict):
    """Storniert Plan Orders (SL/TP), deren Symbol keine offene Position mehr hat."""
    logging.info("Schritt 3: Suche und storniere verwaiste Plan Orders.")
    
    # Symbole mit offenen Positionen
    active_symbols = set(open_positions.keys())
    
    # Symbole mit Conditional Orders
    order_symbols = set(all_plan_orders.keys())
    
    # Symbole, bei denen Conditional Orders existieren, aber keine Position
    orphan_symbols = order_symbols.difference(active_symbols)
    
    if not orphan_symbols:
        logging.info("-> Keine verwaisten Plan Orders gefunden. Alles sauber.")
        return

    storno_count = 0
    
    for symbol in orphan_symbols:
        orders_to_cancel = all_plan_orders[symbol]
        
        logging.warning(f"*** VERWAISTE ORDERS gefunden für {symbol} ({len(orders_to_cancel)} Orders). Storniere...")
        
        for order in orders_to_cancel:
            plan_id = order["planId"]
            trigger = order["triggerPrice"]
            order_type = order.get("orderType", "N/A")
            
            logging.info(f"  -> Storniere Plan-ID {plan_id} (Trigger: {trigger:.4f}, Typ: {order_type})")
            
            await cancel_conditional_order(client, symbol, plan_id)
            storno_count += 1
            
    logging.info(f"ZUSAMMENFASSUNG: {storno_count} verwaiste Plan Orders storniert.")


async def run_sl_correction_check(client: httpx.AsyncClient):
    """Führt den Haupt-Check durch: Abruf, Konsolidierung von SL-Orders, Korrektur und Aufräumen."""
    
    open_positions = await get_all_open_positions(client)
    # Wichtig: all_plan_orders enthält jetzt ALLE Plan Orders, um Aufräumen zu gewährleisten.
    all_plan_orders = await get_sl_and_tp_orders(client) 
    
    if not open_positions:
        logging.info("Keine offenen Positionen gefunden.")
        # Wenn keine Positionen offen sind, sind alle Conditional Orders verwaist.
        await cancel_orphan_plan_orders(client, open_positions, all_plan_orders)
        return

    corrected_count = 0
    missing_sl_symbols = [] 
    
    for symbol, pos_data in open_positions.items():
        current_size = pos_data["size"]
        position_side = pos_data["side"]
        entry_price = pos_data["entry_price"] 
        attached_sl_price = pos_data.get("stop_loss_price") 
        potential_plan_orders = all_plan_orders.get(symbol, [])
        sl_orders_for_symbol = []
        tp_orders_for_symbol = []
        
        if entry_price is not None and entry_price > 0:
            for order in potential_plan_orders:
                # Da get_sl_and_tp_orders jetzt alle Typen sammelt, prüfen wir hier auf Market Orders,
                # die für SL-Größenkorrekturen relevant sind. (Optionale Filterung je nach Strategie)
                if order.get("orderType") != "limit":
                     logging.debug(f"Ignoriere Nicht-Limit-Plan-Order {order['planId']} für {symbol} (Typ: {order['orderType']}) im SL-Check.")
                     continue
                     
                trigger_price = order["triggerPrice"]
                order_side = order["side"] # Order-Side (z.B. close_long)

                # Überprüfen, ob es eine Schließungs-Order ist, die der Position-Side entspricht
                is_closing_order = (position_side == "long" and order_side == "close_long") or \
                                   (position_side == "short" and order_side == "close_short")
                
                if is_closing_order:
                    # Logische Unterscheidung SL vs TP
                    is_sl = (position_side == "long" and trigger_price < entry_price) or \
                            (position_side == "short" and trigger_price > entry_price)
                            
                    if is_sl:
                        # Hier fügen wir die SL-relevanten Orders hinzu, die auf Größenkorrektur geprüft werden
                        sl_orders_for_symbol.append(order)
                    else:
                        # Orders, die dieselbe Seite schließen, aber im Gewinn liegen (TPs)
                        tp_orders_for_symbol.append(order)
                else:
                    # Orders, die die Position eröffnen (z.B. open_long) oder auf der falschen Seite schließen
                    logging.debug(f"Ignoriere Plan Order {order['planId']} für {symbol}: Nicht-Schließungs-Order oder falsche Seite ({order_side}).")
        
        else:
            # --- KORRIGIERTER FALLBACK-BLOCK (umgesetzt wie in der letzten Antwort besprochen) ---
            logging.warning(f"WARNUNG: Einstiegspreis für {symbol} ist NULL oder fehlt. Alle Conditional Closing Market Orders werden als **potenzielle SL** für die Größenkorrektur behandelt.")
            
            # FALLBACK: Wenn Entry Price fehlt, behandeln wir alle Closing Orders als potenziellen SL
            for order in potential_plan_orders:
                 # In diesem Fall sollten wir uns wieder auf Market Orders beschränken, 
                 # da wir nur diese Orders in diesem Script ersetzen und korrigieren wollen.
                if order.get("orderType") != "limit":
                     logging.debug(f"Ignoriere Nicht-Limit-Plan-Order {order['planId']} für {symbol} (Typ: {order['orderType']}) im SL-Check (Entry-Price fehlt).")
                     continue
                     
                order_side = order["side"]
                
                # Wir behandeln alle Closing Orders als SL-Kandidat
                is_closing_order = (position_side == "long" and order_side == "close_long") or \
                                   (position_side == "short" and order_side == "close_short")
                                   
                if is_closing_order:
                    sl_orders_for_symbol.append(order)
                else:
                    # Nicht-Closing Orders ignorieren
                    logging.debug(f"Ignoriere Plan Order {order['planId']} für {symbol}: Nicht-Schließungs-Order.")
        # --- Ende KORRIGIERTER FALLBACK-BLOCK ---

        # --- PRÜFUNG 1: ANGEHÄNGTER SL VORHANDEN? ---
        if attached_sl_price and attached_sl_price > 0:
            logging.info(f"Position {symbol} ist durch einen ANGEHÄNGTEN Stop Loss geschützt (Preis: {attached_sl_price:.4f}).")
            
            # Konfliktvermeidung: Wenn ein angehängter SL existiert, alle Conditional Orders löschen
            if sl_orders_for_symbol:
                logging.warning(f"  -> ACHTUNG: {len(sl_orders_for_symbol)} zusätzliche Conditional SL Orders gefunden. Storniere, um Konflikte zu vermeiden.")
                for old_sl in sl_orders_for_symbol:
                    logging.info(f"  -> Storniere Konflikt-Duplikat: Plan-ID {old_sl['planId']}, Trigger {old_sl['triggerPrice']:.4f}")
                    await cancel_conditional_order(client, symbol, old_sl['planId'])
            
            # Füge auch gefundene TPs hinzu und storniere diese, um Konflikte zu vermeiden
            if tp_orders_for_symbol:
                logging.warning(f"  -> ACHTUNG: {len(tp_orders_for_symbol)} Conditional TP Orders gefunden. Storniere, um Konflikte mit dem angehängten SL/TP zu vermeiden.")
                for old_tp in tp_orders_for_symbol:
                     logging.info(f"  -> Storniere Konflikt-TP: Plan-ID {old_tp['planId']}, Trigger {old_tp['triggerPrice']:.4f}")
                     await cancel_conditional_order(client, symbol, old_tp['planId'])

            continue # Nächste Position prüfen
        
        if tp_orders_for_symbol:
            logging.info(f"-> {len(tp_orders_for_symbol)} Conditional Take Profit Orders gefunden und ignoriert (Kein angehängter SL/TP).")
        
        # --- PRÜFUNG 2: KONSOLIDIERUNG VON MEHRFACH-SL-ORDERS (NUR CONDITIONAL ORDERS) ---
        
        if len(sl_orders_for_symbol) > 1:
            logging.warning(f"*** MEHRFACH-SL FÜR {symbol} ERKANNT ({len(sl_orders_for_symbol)} Conditional Orders). KONSOLIDIERE. ***")
            
            # 1. Optimalen SL finden (höher bei Long, tiefer bei Short = minimaler Verlust)
            if position_side == "long":
                optimal_sl = max(sl_orders_for_symbol, key=lambda x: x['triggerPrice'])
            else: # short
                optimal_sl = min(sl_orders_for_symbol, key=lambda x: x['triggerPrice'])
            
            logging.warning(f"  -> Optimaler SL gewählt: Trigger {optimal_sl['triggerPrice']:.4f}, Plan-ID {optimal_sl['planId']}")
            
            # 2. Alle anderen Conditional Orders stornieren
            orders_to_cancel = [o for o in sl_orders_for_symbol if o['planId'] != optimal_sl['planId']]
            size_to_add = 0.0
            for old_sl in orders_to_cancel:
                size_to_add += old_sl['size']
                logging.info(f"  -> Storniere Duplikat: Plan-ID {old_sl['planId']}, Trigger {old_sl['triggerPrice']:.4f}, Größe: {old_sl['size']:.4f}")
                await cancel_conditional_order(client, symbol, old_sl['planId'])
            
            new_optimal_sl_size = optimal_sl['size'] + size_to_add
            logging.info(f"  -> Konsolidierung: Ursprungsgröße: {optimal_sl['size']:.4f}, Hinzu: {size_to_add:.4f}, Neue SL-Größe: {new_optimal_sl_size:.4f}")
            # Die optimale Order für die anschließende Größenprüfung verwenden
            sl_data = optimal_sl
            sl_data["size"] = new_optimal_sl_size
            
        elif len(sl_orders_for_symbol) == 1:
            sl_data = sl_orders_for_symbol[0]
        else:
            # Keine SL-Orders (weder angehängt noch Conditional) gefunden
            missing_sl_symbols.append(symbol)
            logging.warning(f"Offene Position für {symbol} hat KEINE aktive SL-Order.")
            continue

        # --- PRÜFUNG 3: GRÖSSENKORREKTUR (NUR FÜR DIE ÜBRIGE CONDITIONAL ORDER) ---
        
        registered_sl_size = sl_data["size"]
        
        # Korrigiere nur, wenn die SL-Order-Größe größer ist als die aktuelle Position (+ Toleranz).
        if registered_sl_size > current_size + 0.0001: 
            logging.info(f"*** ABWEICHUNG ERKANNT (Conditional SL ist ZU GROSS) für {symbol} ***")
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
            logging.debug(f"Position für {symbol} ist synchronisiert oder partiell abgedeckt (Conditional SL-Größe: {registered_sl_size:.4f}).")

    if missing_sl_symbols:
        logging.warning(f"ZUSAMMENFASSUNG: Für die folgenden Symbole fehlen aktive Stop-Loss Orders: {', '.join(missing_sl_symbols)}.")

    # Am Ende der Prüfungen: Starte das Aufräumen der Waisen-Orders.
    await cancel_orphan_plan_orders(client, open_positions, all_plan_orders)
    
    logging.info(f"Polling-Durchlauf abgeschlossen. Korrigierte SL-Orders: {corrected_count}.")


async def main_loop():
    """Führt einen einzelnen Durchlauf des Position Handlers aus."""
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