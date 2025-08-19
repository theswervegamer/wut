
from pathlib import Path
import sqlite3, sys


DB_PATH = Path(__file__).with_name("data") / "wut.db"


def add(name: str, gender: str, active: str) -> None:
gender = gender.capitalize()
if gender not in {"Male", "Female"}:
raise SystemExit("Gender must be Male or Female")
active_val = 1 if active.lower() in {"yes", "y", "1", "true"} else 0
conn = sqlite3.connect(DB_PATH)
try:
conn.execute(
"INSERT INTO wrestlers(name, gender, active) VALUES (?,?,?)",
(name, gender, active_val),
)
conn.commit()
finally:
conn.close()
print(f"Added: {name} ({gender}), Active={bool(active_val)}")


if __name__ == "__main__":
if len(sys.argv) != 4:
print('Usage: python add_wrestler.py "Name" Male|Female Yes|No')
raise SystemExit(1)
_, name, gender, active = sys.argv
add(name, gender, active)