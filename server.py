import os
import json
from datetime import datetime, timezone, timedelta
from flask import Flask, request, jsonify
from flask_cors import CORS
import firebase_admin
from firebase_admin import credentials, db

# =====================================================
#              CONFIG (ENV-BASED)
# =====================================================

FIREBASE_DATABASE_URL = os.getenv("FIREBASE_DATABASE_URL")
FIREBASE_SERVICE_ACCOUNT_JSON = os.getenv("FIREBASE_SERVICE_ACCOUNT_JSON")

if not FIREBASE_DATABASE_URL:
    raise RuntimeError("Missing FIREBASE_DATABASE_URL env var")

if not FIREBASE_SERVICE_ACCOUNT_JSON:
    raise RuntimeError("Missing FIREBASE_SERVICE_ACCOUNT_JSON env var")

service_account_info = json.loads(FIREBASE_SERVICE_ACCOUNT_JSON)

# =====================================================
#              FIREBASE INITIALIZATION
# =====================================================

if not firebase_admin._apps:
    cred = credentials.Certificate(service_account_info)
    firebase_admin.initialize_app(cred, {
        "databaseURL": FIREBASE_DATABASE_URL
    })

# =====================================================
#                       FLASK APP
# =====================================================

IST = timezone(timedelta(hours=5, minutes=30))
app = Flask(__name__)
CORS(app)  # allow calls from Android / web

# =====================================================
#                       HELPERS
# =====================================================

def ensure_user_structure(curr):
    if curr is None:
        return {
            "AllStats": {
                "TotalLoans": 0,
                "ActiveLoans": 0,
                "ClosedLoans": 0
            },
            "ClientData": {},
            "_lastBatch": None
        }

    if "AllStats" not in curr:
        curr["AllStats"] = {"TotalLoans": 0, "ActiveLoans": 0, "ClosedLoans": 0}
    if "ClientData" not in curr:
        curr["ClientData"] = {}
    if "_lastBatch" not in curr:
        curr["_lastBatch"] = None

    return curr

def find_today_entry(collection_data, today_date):
    for k, entry in (collection_data or {}).items():
        if entry.get("date") == today_date:
            return k
    return None

def get_next_client_id(client_data: dict):
    i = 1
    while f"P{i}" in client_data:
        i += 1
    return f"P{i}"

# =====================================================
#               SAFE TRANSACTIONS
# =====================================================

def txn_add_new_client(user, lend_date, collection_day):
    user_ref = db.reference(f"Users/{user}")

    def tx(curr):
        if curr is None:
            raise RuntimeError("User node does not exist. Refusing to create root automatically.")

        all_stats = curr.get("AllStats")
        client_data = curr.get("ClientData")

        if all_stats is None or client_data is None:
            raise RuntimeError("Corrupt user structure")

        new_pid = get_next_client_id(client_data)
        stat_key = f"{new_pid}Stat"

        new_client = {
            stat_key: {
                "ClientName": new_pid,
                "CollectionDay": collection_day,
                "LendDate": lend_date,
                "Status": "Active",
                "TotalAmountPaid": 0,
                "WeeksPaid": 0
            },
            "collectionData": {}
        }

        client_data[new_pid] = new_client

        all_stats["TotalLoans"] = int(all_stats.get("TotalLoans", 0)) + 1
        all_stats["ActiveLoans"] = int(all_stats.get("ActiveLoans", 0)) + 1
        all_stats["ClosedLoans"] = int(all_stats.get("ClosedLoans", 0))

        curr["ClientData"] = client_data
        curr["AllStats"] = all_stats
        return curr

    user_ref.transaction(tx)
    return {"status": "success"}

def txn_add_entry(user, client_id, amount, date_str, status):
    user_ref = db.reference(f"Users/{user}")

    def tx(curr):
        if curr is None:
            raise RuntimeError("User not found")

        all_stats = curr.get("AllStats")
        client_data = curr.get("ClientData")

        if all_stats is None or client_data is None:
            raise RuntimeError("Corrupt user structure")

        node = client_data.get(client_id)
        if not node:
            raise RuntimeError("Client not found")

        stat = node.get(f"{client_id}Stat")
        if not stat:
            raise RuntimeError("Client stat missing")

        if stat.get("Status") == "Closed":
            raise RuntimeError("Client already closed")

        coll = node.get("collectionData", {})

        weeks = []
        for k in coll.keys():
            if k.startswith("week"):
                try:
                    weeks.append(int(k.replace("week", "")))
                except:
                    pass

        next_week = (max(weeks) if weeks else 0) + 1
        if next_week > 20:
            raise RuntimeError("Max 20 weeks reached")

        coll[f"week{next_week}"] = {
            "Amount": int(amount),
            "date": date_str,
            "entryStatus": status
        }

        node["collectionData"] = coll

        weeks_paid = int(stat.get("WeeksPaid", 0))
        total_paid = int(stat.get("TotalAmountPaid", 0))

        if status == "paid":
            weeks_paid += 1
            total_paid += int(amount)

        stat["WeeksPaid"] = weeks_paid
        stat["TotalAmountPaid"] = total_paid

        if weeks_paid >= 20:
            stat["Status"] = "Closed"
            all_stats["ActiveLoans"] = int(all_stats.get("ActiveLoans", 0)) - 1
            all_stats["ClosedLoans"] = int(all_stats.get("ClosedLoans", 0)) + 1

        node[f"{client_id}Stat"] = stat
        client_data[client_id] = node

        curr["ClientData"] = client_data
        curr["AllStats"] = all_stats
        return curr

    user_ref.transaction(tx)
    return {"status": "ok"}

# =====================================================
#                       ROUTES
# =====================================================

@app.route('/', methods=["GET"])
def Home():
    return jsonify({'status':'ok','msg':'Api is Live'}), 200

@app.route("/healthz", methods=["GET"])
def healthz():
    return jsonify({"status": "ok"}), 200

@app.route("/dashboard", methods=["GET"])
def get_dashboard():
    user = request.args.get("user")
    if not user:
        return jsonify({"error": "Missing user parameter"}), 400

    try:
        user_ref = db.reference(f"Users/{user}")
        data = user_ref.get()

        if data is None:
            return jsonify({"error": "User not found"}), 404

        client_data = data.get("ClientData")
        if client_data is None:
            return jsonify({"error": "ClientData not found for user"}), 500

        total_loans = 0
        active_loans = 0
        closed_loans = 0

        WEEKLY_AMOUNT = 600
        TOTAL_WEEKS = 20
        TOTAL_PER_CLIENT = WEEKLY_AMOUNT * TOTAL_WEEKS  # 12000

        upcoming_collection = 0  # total outstanding across all clients

        for client_id, node in client_data.items():
            stat = node.get(f"{client_id}Stat")
            if not stat:
                continue  # skip broken records safely

            total_loans += 1

            status = str(stat.get("Status", "")).lower()
            if status == "active":
                active_loans += 1
            elif status == "closed":
                closed_loans += 1

            total_paid = int(stat.get("TotalAmountPaid", 0))
            remaining = TOTAL_PER_CLIENT - total_paid
            if remaining < 0:
                remaining = 0  # safety guard

            upcoming_collection += remaining

        weekly_collection = active_loans * WEEKLY_AMOUNT

        result = {
            "TotalLoans": total_loans,
            "ActiveLoans": active_loans,
            "ClosedLoans": closed_loans,
            "WeeklyCollection": weekly_collection,
            "UpComingCollection": upcoming_collection
        }

        return jsonify(result), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/weekly", methods=["GET"])
def get_weekly():
    user = request.args.get("user")
    if not user:
        return jsonify({"error": "Missing user parameter"}), 400

    try:
        user_ref = db.reference(f"Users/{user}")
        data = user_ref.get()

        if data is None:
            return jsonify({"error": "User not found"}), 404

        client_data = data.get("ClientData")
        if client_data is None:
            return jsonify({"error": "ClientData not found for user"}), 500

        WEEKLY_AMOUNT = 600

        # init counters
        result = {
            "MON": {"count": 0, "amount": 0},
            "TUE": {"count": 0, "amount": 0},
            "WED": {"count": 0, "amount": 0},
            "THU": {"count": 0, "amount": 0},
            "FRI": {"count": 0, "amount": 0},
            "SAT": {"count": 0, "amount": 0},
            "SUN": {"count": 0, "amount": 0},
        }

        for client_id, node in client_data.items():
            stat = node.get(f"{client_id}Stat")
            if not stat:
                continue

            status = str(stat.get("Status", "")).lower()
            if status != "active":
                continue  # only active loans count

            day = stat.get("CollectionDay")
            if day not in result:
                continue  # skip invalid data safely

            result[day]["count"] += 1
            result[day]["amount"] += WEEKLY_AMOUNT

        return jsonify(result), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/nextClientId", methods=["GET"])
def get_next_client_id_route():
    user = request.args.get("user")
    if not user:
        return jsonify({"error": "Missing user parameter"}), 400

    try:
        user_ref = db.reference(f"Users/{user}")
        data = user_ref.get()

        # If user or ClientData doesn't exist yet → first client is P1
        if not data or "ClientData" not in data or not data.get("ClientData"):
            return jsonify({
                "nextClientId": "P1"
            }), 200

        client_data = data.get("ClientData", {})

        # Find max existing P number
        max_n = 0
        for key in client_data.keys():
            if key.startswith("P"):
                try:
                    n = int(key[1:])
                    if n > max_n:
                        max_n = n
                except:
                    pass  # ignore malformed keys safely

        next_id = f"P{max_n + 1}"

        return jsonify({
            "nextClientId": next_id
        }), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/addNewClient", methods=["POST"])
def add_new_client():
    data = request.get_json(silent=True)
    if not data:
        return jsonify({"error": "JSON body required"}), 400

    for k in ["user", "lendDate", "collectionDay"]:
        if k not in data:
            return jsonify({"error": f"Missing {k}"}), 400

    try:
        result = txn_add_new_client(
            data["user"],
            data["lendDate"],
            data["collectionDay"]
        )
        return jsonify(result), 201
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/addEntry", methods=["POST"])
def add_entry():
    data = request.get_json(silent=True)
    if not data:
        return jsonify({"error": "JSON body required"}), 400

    for k in ["user", "clientId", "entryAmount", "entryDate", "entryStatus"]:
        if k not in data:
            return jsonify({"error": f"Missing {k}"}), 400

    try:
        result = txn_add_entry(
            data["user"],
            data["clientId"],
            data["entryAmount"],
            data["entryDate"],
            data["entryStatus"]
        )
        return jsonify(result), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/today", methods=["GET"])
def get_today():
    user = request.args.get("user")
    if not user:
        return jsonify({"error": "Missing user parameter"}), 400

    try:
        user_ref = db.reference(f"Users/{user}")   # 🔥 FIXED PATH
        data = user_ref.get() or {}

        client_data = data.get("ClientData", {}) or {}

        now_ist = datetime.now(IST)
        today_date = now_ist.strftime("%Y-%m-%d")
        weekday_map = ["MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN"]
        today_weekday = weekday_map[now_ist.weekday()]

        print("---- /today DEBUG ----")
        print("IST now:", now_ist.isoformat())
        print("today_date:", today_date, "today_weekday:", today_weekday)
        print("ClientData keys:", list(client_data.keys()))

        due = 0
        collected = 0
        pending = 0
        customers = []

        for client_id, node in client_data.items():
            stat = node.get(f"{client_id}Stat") or {}

            status = str(stat.get("Status", "")).lower()
            if status != "active":
                print(f"SKIP {client_id} status:", status)
                continue

            day = stat.get("CollectionDay")
            if day != today_weekday:
                print(f"SKIP {client_id} day:", day, "!= today:", today_weekday)
                continue

            print(f"MATCH {client_id} day:", day)

            amount = 600
            due += amount

            collection_data = node.get("collectionData") or {}

            paid_today = False
            for _, entry in collection_data.items():
                if entry.get("date") == today_date and str(entry.get("entryStatus", "")).lower() == "paid":
                    paid_today = True
                    break

            if paid_today:
                collected += amount
                status_str = "paid"
            else:
                pending += amount
                status_str = "pending"

            customers.append({
                "clientId": client_id,
                "amount": amount,
                "status": status_str
            })

        result = {
            "date": today_date,
            "weekday": today_weekday,
            "summary": {
                "due": due,
                "collected": collected,
                "pending": pending
            },
            "customers": customers
        }

        print("Today result:", result)
        return jsonify(result), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/today/batchMark", methods=["POST"])
def batch_mark_today():
    data = request.get_json(silent=True)
    if not data or "user" not in data:
        return jsonify({"error": "Missing user"}), 400

    user = data["user"].replace(" ", "")
    user_ref = db.reference(f"Users/{user}")

    def tx(curr):
        curr = ensure_user_structure(curr)

        client_data = curr.get("ClientData", {}) or {}
        all_stats = curr.get("AllStats", {}) or {}
        batches = curr.get("batches", {}) or {}

        now_ist = datetime.now(IST)
        today_date = now_ist.strftime("%Y-%m-%d")
        weekday_map = ["MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN"]
        today_weekday = weekday_map[now_ist.weekday()]

        # 🛑 Prevent double batch for same day
        if today_date in batches:
            return curr

        batch_record = {}  # { "P1": {"action": "created", "week": 5}, ... }

        for client_id, node in client_data.items():
            stat = node.get(f"{client_id}Stat")
            if not stat:
                continue

            # Only ACTIVE
            if str(stat.get("Status", "")).lower() != "active":
                continue

            # Only today's weekday
            if stat.get("CollectionDay") != today_weekday:
                continue

            coll = node.get("collectionData", {}) or {}

            # 🔍 Check if today entry already exists
            today_key = find_today_entry(coll, today_date)

            if today_key:
                # Entry exists for today
                entry = coll.get(today_key, {})
                if str(entry.get("entryStatus", "")).lower() == "paid":
                    # Already paid → do nothing
                    continue

                # Update existing entry to paid
                entry["entryStatus"] = "paid"
                coll[today_key] = entry

                # Update stats
                stat["WeeksPaid"] = int(stat.get("WeeksPaid", 0)) + 1
                stat["TotalAmountPaid"] = int(stat.get("TotalAmountPaid", 0)) + 600

                if stat["WeeksPaid"] >= 20:
                    stat["Status"] = "Closed"
                    all_stats["ActiveLoans"] = int(all_stats.get("ActiveLoans", 0)) - 1
                    all_stats["ClosedLoans"] = int(all_stats.get("ClosedLoans", 0)) + 1

                node["collectionData"] = coll
                node[f"{client_id}Stat"] = stat
                client_data[client_id] = node

                # Record for undo
                week_no = int(today_key.replace("week", ""))
                batch_record[client_id] = {"action": "updated", "week": week_no}

            else:
                # ❌ No entry for today → create next week
                weeks = []
                for k in coll.keys():
                    if k.startswith("week"):
                        try:
                            weeks.append(int(k.replace("week", "")))
                        except:
                            pass

                next_week = (max(weeks) if weeks else 0) + 1
                if next_week > 20:
                    continue

                coll[f"week{next_week}"] = {
                    "Amount": 600,
                    "date": today_date,
                    "entryStatus": "paid"
                }

                # Update stats
                stat["WeeksPaid"] = int(stat.get("WeeksPaid", 0)) + 1
                stat["TotalAmountPaid"] = int(stat.get("TotalAmountPaid", 0)) + 600

                if stat["WeeksPaid"] >= 20:
                    stat["Status"] = "Closed"
                    all_stats["ActiveLoans"] = int(all_stats.get("ActiveLoans", 0)) - 1
                    all_stats["ClosedLoans"] = int(all_stats.get("ClosedLoans", 0)) + 1

                node["collectionData"] = coll
                node[f"{client_id}Stat"] = stat
                client_data[client_id] = node

                # Record for undo
                batch_record[client_id] = {"action": "created", "week": next_week}

        # Save batch receipt (even if empty, it prevents double run)
        batches[today_date] = batch_record

        curr["ClientData"] = client_data
        curr["AllStats"] = all_stats
        curr["batches"] = batches

        return curr

    user_ref.transaction(tx)
    return jsonify({"status": "ok"}), 200

@app.route("/today/undoLastBatch", methods=["POST"])
def undo_last_batch():
    data = request.get_json(silent=True)
    if not data or "user" not in data:
        return jsonify({"error": "Missing user"}), 400

    user = data["user"].replace(" ", "")
    user_ref = db.reference(f"Users/{user}")

    def tx(curr):
        curr = ensure_user_structure(curr)

        client_data = curr.get("ClientData", {}) or {}
        all_stats = curr.get("AllStats", {}) or {}
        batches = curr.get("batches", {}) or {}

        if not batches:
            # Nothing to undo
            return curr

        # Get the latest batch by date (ISO date sorts correctly)
        last_date = sorted(batches.keys())[-1]
        record = batches.get(last_date, {})

        for client_id, info in record.items():
            node = client_data.get(client_id)
            if not node:
                continue

            stat = node.get(f"{client_id}Stat")
            coll = node.get("collectionData", {}) or {}

            week_no = info.get("week")
            action = info.get("action")
            week_key = f"week{week_no}"

            if week_key not in coll or not stat:
                continue

            entry = coll.get(week_key, {})
            amount = int(entry.get("Amount", 600))

            # ---- Undo logic based on action ----
            if action == "created":
                # We created this entry in batch → remove it
                del coll[week_key]

                # Rollback stats
                stat["WeeksPaid"] = max(0, int(stat.get("WeeksPaid", 0)) - 1)
                stat["TotalAmountPaid"] = max(0, int(stat.get("TotalAmountPaid", 0)) - amount)

                # If it was closed, reopen
                if stat.get("Status") == "Closed":
                    stat["Status"] = "Active"
                    all_stats["ActiveLoans"] = int(all_stats.get("ActiveLoans", 0)) + 1
                    all_stats["ClosedLoans"] = int(all_stats.get("ClosedLoans", 0)) - 1

            elif action == "updated":
                # We updated existing entry from pending → paid → revert it
                coll[week_key]["entryStatus"] = "pending"

                # Rollback stats
                stat["WeeksPaid"] = max(0, int(stat.get("WeeksPaid", 0)) - 1)
                stat["TotalAmountPaid"] = max(0, int(stat.get("TotalAmountPaid", 0)) - amount)

                if stat.get("Status") == "Closed":
                    stat["Status"] = "Active"
                    all_stats["ActiveLoans"] = int(all_stats.get("ActiveLoans", 0)) + 1
                    all_stats["ClosedLoans"] = int(all_stats.get("ClosedLoans", 0)) - 1

            # Save back
            node["collectionData"] = coll
            node[f"{client_id}Stat"] = stat
            client_data[client_id] = node

        # Remove this batch record so it can't be undone twice
        del batches[last_date]

        curr["ClientData"] = client_data
        curr["AllStats"] = all_stats
        curr["batches"] = batches

        return curr

    user_ref.transaction(tx)
    return jsonify({"status": "ok"}), 200


@app.route("/auth/resolveUser", methods=["GET"])
def resolve_user():
    email = request.args.get("email")
    if not email:
        return jsonify({"error": "missing email"}), 400

    # Firebase RTDB keys cannot contain '_'
    safe_email = email.replace(".", "_")

    ref = db.reference(f"Users/LoginDetails/{safe_email}")
    data = ref.get()

    if not data:
        return jsonify({"error": "user not found"}), 404

    user_id = data.get("userName")
    if not user_id:
        return jsonify({"error": "userId missing"}), 500

    return jsonify({"userId": user_id}), 200

if __name__ == "__main__":
    port = int(os.getenv("PORT", "5050"))  # Render provides PORT
    app.run(host="0.0.0.0", port=port, debug=False)
