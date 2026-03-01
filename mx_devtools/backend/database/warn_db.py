# Copyright (c) 2025 OPPRO.NET Network
import os
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timedelta

class WarnDatabase:
    def __init__(self, base_path):
        self.db_path = os.path.join(base_path, "Datenbanken", "warns.db")
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        self._init_database()

    def _init_database(self):
        """Initialisiert die Datenbank mit den notwendigen Tabellen"""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS warns (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    guild_id INTEGER NOT NULL,
                    user_id INTEGER NOT NULL,
                    moderator_id INTEGER NOT NULL,
                    reason TEXT NOT NULL,
                    timestamp TEXT NOT NULL
                )
            """)
            conn.commit()

    @contextmanager
    def _get_connection(self):
        conn = sqlite3.connect(self.db_path)
        try:
            yield conn
        finally:
            conn.close()

    # --- Moderations-Logik ---

    def add_warning(self, guild_id, user_id, moderator_id, reason):
        """Fügt eine Warnung hinzu (Zeitstempel wird automatisch generiert)"""
        timestamp = datetime.now().isoformat()
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "INSERT INTO warns (guild_id, user_id, moderator_id, reason, timestamp) VALUES (?, ?, ?, ?, ?)",
                    (guild_id, user_id, moderator_id, reason, timestamp)
                )
                conn.commit()
                return cursor.lastrowid
        except Exception as e:
            print(f"Error adding warning: {e}")
            raise

    def get_warnings(self, guild_id, user_id):
        """Gibt alle Warnungen eines Nutzers zurück (für /mywarns oder Mod-Ansicht)"""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT id, reason, timestamp, moderator_id FROM warns WHERE guild_id = ? AND user_id = ? ORDER BY id DESC",
                (guild_id, user_id)
            )
            return cursor.fetchall()

    # --- Privacy & Maintenance (Update auf 180 Tage) ---

    def delete_user_data(self, user_id: int) -> bool:
        """DSGVO Hard-Delete: Löscht ALLE Warnungen eines Nutzers global."""
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("DELETE FROM warns WHERE user_id = ?", (user_id,))
                conn.commit()
                return True
        except Exception as e:
            print(f"Hard Delete Error: {e}")
            return False

    def cleanup_old_data(self, days: int = 180) -> int:
        """
        Rolling Cleanup: Entfernt Warnungen, die älter als 'days' (Standard 180) sind.
        Dies schützt vor unendlicher Datenspeicherung.
        """
        cutoff = (datetime.now() - timedelta(days=days)).isoformat()
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("DELETE FROM warns WHERE timestamp < ?", (cutoff,))
                count = cursor.rowcount
                conn.commit()
                if count > 0:
                    print(f"[Cleanup] {count} Warnungen (älter als {days} Tage) wurden gelöscht.")
                return count
        except Exception as e:
            print(f"Cleanup Error: {e}")
            return 0