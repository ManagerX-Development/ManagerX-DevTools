# Copyright (c) 2026 OPPRO.NET Network
import sqlite3
import os
import logging
from typing import Optional, List, Dict, Tuple
from datetime import datetime

# Logger
logger = logging.getLogger(__name__)

DB_PATH = "data/economy.db"

class EconomyDatabase:
    """
    Database manager for the ManagerX Economy system (Global and Guild-specific).
    """
    
    def __init__(self):
        self._ensure_db_dir()
        self.create_tables()
        self._seed_shop()

    def _ensure_db_dir(self):
        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

    def _get_connection(self):
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        return conn

    def create_tables(self):
        """Create all required tables for the economy system."""
        try:
            with self._get_connection() as conn:
                c = conn.cursor()

                # Global Economy
                c.execute("""
                    CREATE TABLE IF NOT EXISTS global_economy (
                        user_id INTEGER PRIMARY KEY,
                        coins INTEGER DEFAULT 0,
                        total_earned INTEGER DEFAULT 0,
                        last_daily TIMESTAMP,
                        last_message_at TIMESTAMP
                    )
                """)

                # Guild Economy
                c.execute("""
                    CREATE TABLE IF NOT EXISTS guild_economy (
                        guild_id INTEGER,
                        user_id INTEGER,
                        coins INTEGER DEFAULT 0,
                        total_earned INTEGER DEFAULT 0,
                        PRIMARY KEY (guild_id, user_id)
                    )
                """)

                # Shop Items
                c.execute("""
                    CREATE TABLE IF NOT EXISTS shop_items (
                        item_id INTEGER PRIMARY KEY AUTOINCREMENT,
                        name TEXT NOT NULL,
                        description TEXT,
                        price INTEGER NOT NULL,
                        type TEXT NOT NULL, -- 'color', 'emoji', 'title'
                        value TEXT NOT NULL, -- hex code, emoji string, etc.
                        is_active BOOLEAN DEFAULT 1
                    )
                """)

                # User Inventory
                c.execute("""
                    CREATE TABLE IF NOT EXISTS user_inventory (
                        user_id INTEGER,
                        item_id INTEGER,
                        purchased_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        is_equipped BOOLEAN DEFAULT 0,
                        PRIMARY KEY (user_id, item_id),
                        FOREIGN KEY (item_id) REFERENCES shop_items(item_id)
                    )
                """)

                conn.commit()
                logger.info("✅ Economy database tables created")
        except sqlite3.Error as e:
            logger.error(f"❌ Error creating economy tables: {e}")
            raise

    def _seed_shop(self):
        """Seed the shop with some initial items if empty."""
        try:
            with self._get_connection() as conn:
                c = conn.cursor()
                c.execute("SELECT COUNT(*) FROM shop_items")
                if c.fetchone()[0] == 0:
                    items = [
                        ('Rot (Name)', 'Verändert die Farbe deines Namens im GlobalChat zu Rot.', 500, 'color', '#FF0000'),
                        ('Blau (Name)', 'Verändert die Farbe deines Namens im GlobalChat zu Blau.', 500, 'color', '#3498DB'),
                        ('Grün (Name)', 'Verändert die Farbe deines Namens im GlobalChat zu Grün.', 500, 'color', '#2ECC71'),
                        ('Gold (Name)', 'Premium-Farbe für deinen Namen.', 2000, 'color', '#F1C40F'),
                        ('🔥 Feuer-Emoji', 'Fügt ein Feuer-Emoji neben deinen Namen.', 300, 'emoji', '🔥'),
                        ('👑 Kronen-Emoji', 'Fügt eine Krone neben deinen Namen.', 1000, 'emoji', '👑'),
                        ('⚡ Blitz-Emoji', 'Fügt einen Blitz neben deinen Namen.', 300, 'emoji', '⚡')
                    ]
                    c.executemany("INSERT INTO shop_items (name, description, price, type, value) VALUES (?, ?, ?, ?, ?)", items)
                    conn.commit()
                    logger.info("✅ Shop seeded with initial items")
        except sqlite3.Error as e:
            logger.error(f"❌ Error seeding shop: {e}")

    # --- Global Economy Methods ---

    def get_global_balance(self, user_id: int) -> int:
        try:
            with self._get_connection() as conn:
                c = conn.cursor()
                c.execute("SELECT coins FROM global_economy WHERE user_id = ?", (user_id,))
                result = c.fetchone()
                return result['coins'] if result else 0
        except sqlite3.Error:
            return 0

    def add_global_coins(self, user_id: int, amount: int):
        try:
            with self._get_connection() as conn:
                c = conn.cursor()
                c.execute("""
                    INSERT INTO global_economy (user_id, coins, total_earned) 
                    VALUES (?, ?, ?)
                    ON CONFLICT(user_id) DO UPDATE SET 
                    coins = coins + ?, 
                    total_earned = total_earned + ?
                """, (user_id, amount, amount, amount, amount))
                conn.commit()
        except sqlite3.Error as e:
            logger.error(f"❌ Error adding global coins: {e}")

    def claim_daily(self, user_id: int, amount: int) -> bool:
        try:
            with self._get_connection() as conn:
                c = conn.cursor()
                c.execute("UPDATE global_economy SET coins = coins + ?, last_daily = CURRENT_TIMESTAMP WHERE user_id = ?", (amount, user_id))
                if c.rowcount == 0:
                    c.execute("INSERT INTO global_economy (user_id, coins, last_daily) VALUES (?, ?, CURRENT_TIMESTAMP)", (user_id, amount))
                conn.commit()
                return True
        except sqlite3.Error:
            return False

    def update_last_message(self, user_id: int):
        try:
            with self._get_connection() as conn:
                c = conn.cursor()
                c.execute("""
                    INSERT INTO global_economy (user_id, last_message_at) 
                    VALUES (?, CURRENT_TIMESTAMP)
                    ON CONFLICT(user_id) DO UPDATE SET last_message_at = CURRENT_TIMESTAMP
                """, (user_id,))
                conn.commit()
        except sqlite3.Error as e:
            logger.error(f"❌ Error updating last message: {e}")

    def get_user_economy_info(self, user_id: int) -> Dict:
        try:
            with self._get_connection() as conn:
                c = conn.cursor()
                c.execute("SELECT * FROM global_economy WHERE user_id = ?", (user_id,))
                result = c.fetchone()
                return dict(result) if result else {}
        except sqlite3.Error:
            return {}

    # --- Guild Economy Methods ---

    def get_guild_balance(self, guild_id: int, user_id: int) -> int:
        try:
            with self._get_connection() as conn:
                c = conn.cursor()
                c.execute("SELECT coins FROM guild_economy WHERE guild_id = ? AND user_id = ?", (guild_id, user_id))
                result = c.fetchone()
                return result['coins'] if result else 0
        except sqlite3.Error:
            return 0

    def add_guild_coins(self, guild_id: int, user_id: int, amount: int):
        try:
            with self._get_connection() as conn:
                c = conn.cursor()
                c.execute("""
                    INSERT INTO guild_economy (guild_id, user_id, coins, total_earned) 
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(guild_id, user_id) DO UPDATE SET 
                    coins = coins + ?, 
                    total_earned = total_earned + ?
                """, (guild_id, user_id, amount, amount, amount, amount))
                conn.commit()
        except sqlite3.Error as e:
            logger.error(f"❌ Error adding guild coins: {e}")

    # --- Shop & Inventory Methods ---

    def get_shop_items(self) -> List[Dict]:
        try:
            with self._get_connection() as conn:
                c = conn.cursor()
                c.execute("SELECT * FROM shop_items WHERE is_active = 1")
                return [dict(row) for row in c.fetchall()]
        except sqlite3.Error:
            return []

    def get_item(self, item_id: int) -> Optional[Dict]:
        try:
            with self._get_connection() as conn:
                c = conn.cursor()
                c.execute("SELECT * FROM shop_items WHERE item_id = ?", (item_id,))
                result = c.fetchone()
                return dict(result) if result else None
        except sqlite3.Error:
            return None

    def buy_item(self, user_id: int, item_id: int) -> Tuple[bool, str]:
        item = self.get_item(item_id)
        if not item:
            return False, "Item existiert nicht."
        
        balance = self.get_global_balance(user_id)
        if balance < item['price']:
            return False, f"Nicht genug Coins. Du brauchst {item['price']} Coins."
        
        try:
            with self._get_connection() as conn:
                c = conn.cursor()
                # Deduct coins
                c.execute("UPDATE global_economy SET coins = coins - ? WHERE user_id = ?", (item['price'], user_id))
                # Add to inventory
                c.execute("INSERT OR IGNORE INTO user_inventory (user_id, item_id) VALUES (?, ?)", (user_id, item_id))
                conn.commit()
                return True, f"Du hast '{item['name']}' erfolgreich gekauft!"
        except sqlite3.Error as e:
            logger.error(f"❌ Error buying item: {e}")
            return False, "Ein Datenbankfehler ist aufgetreten."

    def get_user_inventory(self, user_id: int) -> List[Dict]:
        try:
            with self._get_connection() as conn:
                c = conn.cursor()
                c.execute("""
                    SELECT si.*, ui.purchased_at, ui.is_equipped 
                    FROM shop_items si
                    JOIN user_inventory ui ON si.item_id = ui.item_id
                    WHERE ui.user_id = ?
                """, (user_id,))
                return [dict(row) for row in c.fetchall()]
        except sqlite3.Error:
            return []

    def equip_item(self, user_id: int, item_id: int) -> bool:
        try:
            with self._get_connection() as conn:
                c = conn.cursor()
                
                # Check if user owns item
                c.execute("SELECT type FROM shop_items WHERE item_id = ?", (item_id,))
                item = c.fetchone()
                if not item: return False
                item_type = item['type']

                # Unequip other items of the same type
                c.execute("""
                    UPDATE user_inventory 
                    SET is_equipped = 0 
                    WHERE user_id = ? AND item_id IN (SELECT item_id FROM shop_items WHERE type = ?)
                """, (user_id, item_type))
                
                # Equip new item
                c.execute("UPDATE user_inventory SET is_equipped = 1 WHERE user_id = ? AND item_id = ?", (user_id, item_id))
                conn.commit()
                return True
        except sqlite3.Error:
            return False

    def get_equipped_overrides(self, user_id: int) -> Dict[str, str]:
        """Returns a dict of equipped overrides like {'color': '#FF0000', 'emoji': '🔥'}"""
        try:
            with self._get_connection() as conn:
                c = conn.cursor()
                c.execute("""
                    SELECT si.type, si.value 
                    FROM shop_items si
                    JOIN user_inventory ui ON si.item_id = ui.item_id
                    WHERE ui.user_id = ? AND ui.is_equipped = 1
                """, (user_id,))
                return {row['type']: row['value'] for row in c.fetchall()}
        except sqlite3.Error:
            return {}

    def delete_user_data(self, user_id: int):
        try:
            with self._get_connection() as conn:
                c = conn.cursor()
                c.execute("DELETE FROM global_economy WHERE user_id = ?", (user_id,))
                c.execute("DELETE FROM guild_economy WHERE user_id = ?", (user_id,))
                c.execute("DELETE FROM user_inventory WHERE user_id = ?", (user_id,))
                conn.commit()
        except sqlite3.Error as e:
            logger.error(f"❌ Error deleting user economy data: {e}")
