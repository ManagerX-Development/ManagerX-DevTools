# database.py
import sqlite3
import json
from datetime import datetime
from typing import Optional, List, Dict, Any

class ProfileDB:
    def __init__(self, db_path="data/profiles.db"):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.cursor = self.conn.cursor()
        self._create_tables()
    
    def _create_tables(self):
        """Erstelle alle benötigten Tabellen"""
        
        # Profile Tabelle
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS profiles (
                user_id INTEGER PRIMARY KEY,
                username TEXT NOT NULL,
                bio TEXT DEFAULT '',
                color TEXT DEFAULT '#5865F2',
                banner TEXT,
                theme TEXT DEFAULT 'default',
                privacy TEXT DEFAULT 'public',
                level INTEGER DEFAULT 1,
                xp INTEGER DEFAULT 0,
                xp_needed INTEGER DEFAULT 100,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
        ''')
        
        # Links Tabelle
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS profile_links (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                name TEXT NOT NULL,
                url TEXT NOT NULL,
                emoji TEXT DEFAULT '🔗',
                position INTEGER DEFAULT 0,
                FOREIGN KEY (user_id) REFERENCES profiles(user_id) ON DELETE CASCADE
            )
        ''')
        
        # Achievements Tabelle
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS achievements (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                name TEXT NOT NULL,
                description TEXT,
                icon TEXT,
                unlocked_at TEXT NOT NULL,
                FOREIGN KEY (user_id) REFERENCES profiles(user_id) ON DELETE CASCADE
            )
        ''')
        
        # Marketplace Tabelle
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS marketplace (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                description TEXT,
                tags TEXT,
                author_id INTEGER NOT NULL,
                author_name TEXT NOT NULL,
                profile_data TEXT NOT NULL,
                downloads INTEGER DEFAULT 0,
                rating REAL DEFAULT 0.0,
                created_at TEXT NOT NULL
            )
        ''')
        
        # Marketplace Downloads Tabelle (wer hat was runtergeladen)
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS marketplace_downloads (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                marketplace_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                downloaded_at TEXT NOT NULL,
                FOREIGN KEY (marketplace_id) REFERENCES marketplace(id) ON DELETE CASCADE
            )
        ''')
        
        # Marketplace Ratings Tabelle
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS marketplace_ratings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                marketplace_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                rating INTEGER NOT NULL,
                rated_at TEXT NOT NULL,
                UNIQUE(marketplace_id, user_id),
                FOREIGN KEY (marketplace_id) REFERENCES marketplace(id) ON DELETE CASCADE
            )
        ''')
        
        self.conn.commit()
    
    # ===== HELPER FUNKTIONEN =====
    
    def _row_to_dict(self, row) -> Dict:
        """Konvertiere sqlite3.Row zu Dictionary"""
        if row is None:
            return None
        return dict(row)
    
    # ===== PROFILE FUNKTIONEN =====
    
    def get_profile(self, user_id: int) -> Optional[Dict]:
        """Hole Profil eines Users mit allen Links"""
        self.cursor.execute('SELECT * FROM profiles WHERE user_id = ?', (user_id,))
        profile = self._row_to_dict(self.cursor.fetchone())
        
        if not profile:
            return None
        
        # Hole Links
        self.cursor.execute('''
            SELECT name, url, emoji 
            FROM profile_links 
            WHERE user_id = ? 
            ORDER BY position
        ''', (user_id,))
        
        profile['links'] = [self._row_to_dict(row) for row in self.cursor.fetchall()]
        
        # Hole Achievements
        self.cursor.execute('''
            SELECT name, description, icon, unlocked_at 
            FROM achievements 
            WHERE user_id = ?
            ORDER BY unlocked_at DESC
        ''', (user_id,))
        
        profile['achievements'] = [self._row_to_dict(row) for row in self.cursor.fetchall()]
        
        return profile
    
    def create_profile(self, user_id: int, username: str) -> Dict:
        """Erstelle neues Profil"""
        now = datetime.now().isoformat()
        
        self.cursor.execute('''
            INSERT INTO profiles (user_id, username, created_at, updated_at)
            VALUES (?, ?, ?, ?)
        ''', (user_id, username, now, now))
        
        self.conn.commit()
        return self.get_profile(user_id)
    
    def update_profile_setting(self, user_id: int, key: str, value: Any) -> bool:
        """Update einzelne Einstellung im Profil"""
        allowed_keys = ['bio', 'color', 'banner', 'theme', 'privacy', 'level', 'xp', 'xp_needed', 'username']
        
        if key not in allowed_keys:
            return False
        
        now = datetime.now().isoformat()
        query = f'UPDATE profiles SET {key} = ?, updated_at = ? WHERE user_id = ?'
        
        self.cursor.execute(query, (value, now, user_id))
        self.conn.commit()
        return self.cursor.rowcount > 0
    
    def delete_profile(self, user_id: int) -> bool:
        """Lösche Profil (CASCADE löscht automatisch Links und Achievements)"""
        self.cursor.execute('DELETE FROM profiles WHERE user_id = ?', (user_id,))
        self.conn.commit()
        return self.cursor.rowcount > 0
    
    # ===== LINKS FUNKTIONEN =====
    
    def add_profile_link(self, user_id: int, link_data: Dict) -> bool:
        """Füge Link zum Profil hinzu"""
        # Zähle aktuelle Links
        self.cursor.execute('SELECT COUNT(*) as count FROM profile_links WHERE user_id = ?', (user_id,))
        count = self.cursor.fetchone()['count']
        
        if count >= 5:
            return False
        
        self.cursor.execute('''
            INSERT INTO profile_links (user_id, name, url, emoji, position)
            VALUES (?, ?, ?, ?, ?)
        ''', (user_id, link_data['name'], link_data['url'], link_data.get('emoji', '🔗'), count))
        
        self.conn.commit()
        return True
    
    def delete_profile_link(self, user_id: int, link_index: int) -> bool:
        """Lösche Link anhand des Index"""
        self.cursor.execute('''
            DELETE FROM profile_links 
            WHERE user_id = ? AND id = (
                SELECT id FROM profile_links 
                WHERE user_id = ? 
                ORDER BY position 
                LIMIT 1 OFFSET ?
            )
        ''', (user_id, user_id, link_index))
        
        self.conn.commit()
        return self.cursor.rowcount > 0
    
    def get_profile_links(self, user_id: int) -> List[Dict]:
        """Hole alle Links eines Profils"""
        self.cursor.execute('''
            SELECT name, url, emoji 
            FROM profile_links 
            WHERE user_id = ? 
            ORDER BY position
        ''', (user_id,))
        
        return [self._row_to_dict(row) for row in self.cursor.fetchall()]
    
    # ===== ACHIEVEMENTS FUNKTIONEN =====
    
    def add_achievement(self, user_id: int, name: str, description: str = "", icon: str = "🏆") -> bool:
        """Füge Achievement hinzu"""
        now = datetime.now().isoformat()
        
        self.cursor.execute('''
            INSERT INTO achievements (user_id, name, description, icon, unlocked_at)
            VALUES (?, ?, ?, ?, ?)
        ''', (user_id, name, description, icon, now))
        
        self.conn.commit()
        return True
    
    def get_achievements(self, user_id: int) -> List[Dict]:
        """Hole alle Achievements eines Users"""
        self.cursor.execute('''
            SELECT name, description, icon, unlocked_at 
            FROM achievements 
            WHERE user_id = ?
            ORDER BY unlocked_at DESC
        ''', (user_id,))
        
        return [self._row_to_dict(row) for row in self.cursor.fetchall()]
    
    # ===== MARKETPLACE FUNKTIONEN =====
    
    def add_to_marketplace(self, marketplace_data: Dict) -> int:
        """Füge Profil zum Marketplace hinzu"""
        self.cursor.execute('''
            INSERT INTO marketplace 
            (name, description, tags, author_id, author_name, profile_data, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (
            marketplace_data['name'],
            marketplace_data['description'],
            json.dumps(marketplace_data['tags']),
            marketplace_data['author_id'],
            marketplace_data['author_name'],
            json.dumps(marketplace_data['profile_data']),
            marketplace_data['created_at']
        ))
        
        self.conn.commit()
        return self.cursor.lastrowid
    
    def get_marketplace_profiles(self, search: Optional[str] = None) -> List[Dict]:
        """Hole alle Marketplace Profile (mit optionaler Suche)"""
        if search:
            self.cursor.execute('''
                SELECT id, name, description, tags, author_id, author_name, 
                       downloads, rating, created_at
                FROM marketplace
                WHERE name LIKE ? OR description LIKE ? OR tags LIKE ?
                ORDER BY downloads DESC, rating DESC
            ''', (f'%{search}%', f'%{search}%', f'%{search}%'))
        else:
            self.cursor.execute('''
                SELECT id, name, description, tags, author_id, author_name, 
                       downloads, rating, created_at
                FROM marketplace
                ORDER BY downloads DESC, rating DESC
            ''')
        
        profiles = []
        for row in self.cursor.fetchall():
            profile = self._row_to_dict(row)
            profile['tags'] = json.loads(profile['tags'])
            profiles.append(profile)
        
        return profiles
    
    def get_marketplace_profile(self, profile_id: int) -> Optional[Dict]:
        """Hole einzelnes Marketplace Profil mit voller profile_data"""
        self.cursor.execute('SELECT * FROM marketplace WHERE id = ?', (profile_id,))
        profile = self._row_to_dict(self.cursor.fetchone())
        
        if not profile:
            return None
        
        profile['tags'] = json.loads(profile['tags'])
        profile['profile_data'] = json.loads(profile['profile_data'])
        
        return profile
    
    def download_marketplace_profile(self, marketplace_id: int, user_id: int) -> bool:
        """Markiere Profil als heruntergeladen und erhöhe Download-Counter"""
        now = datetime.now().isoformat()
        
        # Prüfe ob bereits heruntergeladen
        self.cursor.execute('''
            SELECT id FROM marketplace_downloads 
            WHERE marketplace_id = ? AND user_id = ?
        ''', (marketplace_id, user_id))
        
        if self.cursor.fetchone():
            return False  # Bereits heruntergeladen
        
        # Füge Download hinzu
        self.cursor.execute('''
            INSERT INTO marketplace_downloads (marketplace_id, user_id, downloaded_at)
            VALUES (?, ?, ?)
        ''', (marketplace_id, user_id, now))
        
        # Erhöhe Download Counter
        self.cursor.execute('''
            UPDATE marketplace 
            SET downloads = downloads + 1 
            WHERE id = ?
        ''', (marketplace_id,))
        
        self.conn.commit()
        return True
    
    def rate_marketplace_profile(self, marketplace_id: int, user_id: int, rating: int) -> bool:
        """Bewerte Marketplace Profil (1-5 Sterne)"""
        if not 1 <= rating <= 5:
            return False
        
        now = datetime.now().isoformat()
        
        # Insert or Replace Rating
        self.cursor.execute('''
            INSERT OR REPLACE INTO marketplace_ratings 
            (marketplace_id, user_id, rating, rated_at)
            VALUES (?, ?, ?, ?)
        ''', (marketplace_id, user_id, rating, now))
        
        # Berechne Durchschnitts-Rating
        self.cursor.execute('''
            SELECT AVG(rating) as avg_rating 
            FROM marketplace_ratings 
            WHERE marketplace_id = ?
        ''', (marketplace_id,))
        
        avg_rating = self.cursor.fetchone()['avg_rating']
        
        # Update Marketplace Rating
        self.cursor.execute('''
            UPDATE marketplace 
            SET rating = ? 
            WHERE id = ?
        ''', (round(avg_rating, 1), marketplace_id))
        
        self.conn.commit()
        return True
    
    def get_user_uploads(self, user_id: int) -> List[Dict]:
        """Hole alle vom User hochgeladenen Profile"""
        self.cursor.execute('''
            SELECT id, name, description, downloads, rating, created_at
            FROM marketplace
            WHERE author_id = ?
            ORDER BY created_at DESC
        ''', (user_id,))
        
        return [self._row_to_dict(row) for row in self.cursor.fetchall()]
    
    def delete_marketplace_profile(self, profile_id: int, user_id: int) -> bool:
        """Lösche Marketplace Profil (nur eigene)"""
        self.cursor.execute('''
            DELETE FROM marketplace 
            WHERE id = ? AND author_id = ?
        ''', (profile_id, user_id))
        
        self.conn.commit()
        return self.cursor.rowcount > 0
    
    # ===== XP & LEVEL SYSTEM =====
    
    def add_xp(self, user_id: int, amount: int) -> Dict:
        """Füge XP hinzu und level ggf. auf"""
        profile = self.get_profile(user_id)
        if not profile:
            return None
        
        new_xp = profile['xp'] + amount
        new_level = profile['level']
        xp_needed = profile['xp_needed']
        
        # Level Up Check
        while new_xp >= xp_needed:
            new_xp -= xp_needed
            new_level += 1
            xp_needed = int(xp_needed * 1.5)  # XP für nächstes Level
        
        self.cursor.execute('''
            UPDATE profiles 
            SET xp = ?, level = ?, xp_needed = ?, updated_at = ?
            WHERE user_id = ?
        ''', (new_xp, new_level, xp_needed, datetime.now().isoformat(), user_id))
        
        self.conn.commit()
        
        return {
            'level': new_level,
            'xp': new_xp,
            'xp_needed': xp_needed,
            'leveled_up': new_level > profile['level']
        }
    
    # ===== STATS =====
    
    def get_stats(self) -> Dict:
        """Hole globale Statistiken"""
        self.cursor.execute('SELECT COUNT(*) as count FROM profiles')
        total_profiles = self.cursor.fetchone()['count']
        
        self.cursor.execute('SELECT COUNT(*) as count FROM marketplace')
        total_marketplace = self.cursor.fetchone()['count']
        
        self.cursor.execute('SELECT SUM(downloads) as total FROM marketplace')
        total_downloads = self.cursor.fetchone()['total'] or 0
        
        return {
            'total_profiles': total_profiles,
            'total_marketplace': total_marketplace,
            'total_downloads': total_downloads
        }
    
    # ------------------------------------------------------------------
    # Privacy & Maintenance
    # ------------------------------------------------------------------

    def delete_user_data(self, user_id: int) -> bool:
        """
        Hard Delete – removes ALL profile data for a user.
        Cascades to profile_links, achievements, marketplace_downloads,
        marketplace_ratings, and marketplace listings authored by the user.
        """
        try:
            # CASCADE on profiles table handles profile_links & achievements
            self.cursor.execute('DELETE FROM profiles WHERE user_id = ?', (user_id,))
            # Marketplace: remove authored listings
            self.cursor.execute('DELETE FROM marketplace WHERE author_id = ?', (user_id,))
            # Download & rating history
            self.cursor.execute('DELETE FROM marketplace_downloads WHERE user_id = ?', (user_id,))
            self.cursor.execute('DELETE FROM marketplace_ratings WHERE user_id = ?', (user_id,))
            self.conn.commit()
            return True
        except Exception:
            return False

    def close(self):
        """Schließe Datenbankverbindung"""
        self.conn.close()