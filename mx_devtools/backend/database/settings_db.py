import sqlite3
import os
from datetime import datetime

class SettingsDB:
    """
    Datenbank-Klasse zur Verwaltung von Benutzer- und Servereinstellungen.
    """
    def __init__(self, db_path="data/settings.db"):
        self.db_path = db_path
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        self.conn = sqlite3.connect(self.db_path)
        self.cursor = self.conn.cursor()
        self.create_tables()
        print(f"[{datetime.now().strftime('%H:%M:%S')}] [DATABASE] Settings Database initialized ✓")

    def create_tables(self):
        """Erstellt die notwendigen Tabellen, falls sie nicht existieren."""
        # Benutzereinstellungen
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS user_settings (
                user_id INTEGER PRIMARY KEY,
                language TEXT NOT NULL DEFAULT 'en'
            )
        """)
        
        # Gilden-spezifische Einstellungen (NEU)
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS guild_settings (
                guild_id INTEGER PRIMARY KEY,
                user_role_id INTEGER,
                team_role_id INTEGER,
                language TEXT NOT NULL DEFAULT 'de'
            )
        """)
        self.conn.commit()

    def set_user_language(self, user_id: int, lang_code: str):
        """Speichert den Sprachcode für einen Benutzer."""
        self.cursor.execute("""
            INSERT OR REPLACE INTO user_settings (user_id, language)
            VALUES (?, ?)
        """, (user_id, lang_code))
        self.conn.commit()

    def get_user_language(self, user_id: int) -> str:
        """Ruft den Sprachcode für einen Benutzer ab. Standard: 'en'."""
        self.cursor.execute("SELECT language FROM user_settings WHERE user_id = ?", (user_id,))
        result = self.cursor.fetchone()
        
        # 'en' als gewünschter Standard, falls kein Eintrag gefunden wird
        return result[0] if result else 'en'

    # ------------------------------------------------------------------
    # Guild Settings (NEU)
    # ------------------------------------------------------------------

    def get_guild_settings(self, guild_id: int) -> dict:
        """Ruft alle Einstellungen für eine Gilde ab."""
        self.cursor.execute("SELECT user_role_id, team_role_id, language FROM guild_settings WHERE guild_id = ?", (guild_id,))
        result = self.cursor.fetchone()
        if result:
            return {
                "user_role_id": result[0],
                "team_role_id": result[1],
                "language": result[2]
            }
        return {"user_role_id": None, "team_role_id": None, "language": "de"}

    def update_guild_settings(self, guild_id: int, **kwargs):
        """Aktualisiert spezifische Gilden-Einstellungen."""
        # Zuerst prüfen, ob Eintrag existiert
        self.cursor.execute("SELECT guild_id FROM guild_settings WHERE guild_id = ?", (guild_id,))
        if not self.cursor.fetchone():
            self.cursor.execute("INSERT INTO guild_settings (guild_id) VALUES (?)", (guild_id,))
        
        for key, value in kwargs.items():
            if key in ["user_role_id", "team_role_id", "language"]:
                self.cursor.execute(f"UPDATE guild_settings SET {key} = ? WHERE guild_id = ?", (value, guild_id))
        
        self.conn.commit()

    def get_guild_language(self, guild_id: int) -> str:
        """Gibt die Sprache einer Gilde zurück."""
        settings = self.get_guild_settings(guild_id)
        return settings.get("language", "de")

    def set_guild_language(self, guild_id: int, lang_code: str):
        """Setzt die Sprache einer Gilde."""
        self.update_guild_settings(guild_id, language=lang_code)

    # ------------------------------------------------------------------
    # Privacy & Maintenance
    # ------------------------------------------------------------------

    def delete_user_data(self, user_id: int) -> bool:
        """Hard Delete – removes ALL settings data for a user."""
        try:
            self.cursor.execute("DELETE FROM user_settings WHERE user_id = ?", (user_id,))
            self.conn.commit()
            return True
        except Exception:
            return False

    def close(self):
        self.conn.close()