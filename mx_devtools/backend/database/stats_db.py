# Copyright (c) 2025 OPPRO.NET Network
import sqlite3
import asyncio
import json
import math
from datetime import datetime, timedelta
from typing import Optional, List, Tuple, Dict
import logging

logger = logging.getLogger(__name__)


class StatsDB:

    def __init__(self, db_file="data/stats.db"):
        self.db_file = db_file
        self.conn = sqlite3.connect(db_file, check_same_thread=False)
        self.cursor = self.conn.cursor()
        self.lock = asyncio.Lock()
        self._create_tables()

    # ------------------------------------------------------------------
    # Schema
    # ------------------------------------------------------------------

    def _create_tables(self):
        """Create all necessary tables for privacy-first stats tracking."""
        tables = [
            # Raw event log – wiped monthly + rolling 30-day cleanup.
            '''CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                guild_id INTEGER NOT NULL,
                channel_id INTEGER NOT NULL,
                message_id INTEGER NOT NULL,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                word_count INTEGER DEFAULT 0,
                has_attachment BOOLEAN DEFAULT FALSE,
                message_type TEXT DEFAULT 'text'
            )''',

            # Raw event log – wiped monthly + rolling 30-day cleanup.
            '''CREATE TABLE IF NOT EXISTS voice_sessions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                guild_id INTEGER NOT NULL,
                channel_id INTEGER NOT NULL,
                start_time DATETIME DEFAULT CURRENT_TIMESTAMP,
                end_time DATETIME,
                duration_minutes REAL DEFAULT 0
            )''',

            # Long-lived global level data – NOT reset monthly.
            '''CREATE TABLE IF NOT EXISTS global_user_levels (
                user_id INTEGER PRIMARY KEY,
                global_level INTEGER DEFAULT 1,
                global_xp INTEGER DEFAULT 0,
                total_messages INTEGER DEFAULT 0,
                total_voice_minutes INTEGER DEFAULT 0,
                total_servers INTEGER DEFAULT 0,
                first_seen DATETIME DEFAULT CURRENT_TIMESTAMP,
                last_activity DATETIME DEFAULT CURRENT_TIMESTAMP,
                achievements TEXT DEFAULT '[]',
                daily_streak INTEGER DEFAULT 0,
                best_streak INTEGER DEFAULT 0,
                last_daily_activity DATE,
                is_private BOOLEAN DEFAULT 0
            )''',

            # ANONYMIZED: aggregated per guild+date, no user_id stored.
            '''CREATE TABLE IF NOT EXISTS daily_stats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                guild_id INTEGER NOT NULL,
                date DATE NOT NULL,
                messages_count INTEGER DEFAULT 0,
                voice_minutes REAL DEFAULT 0,
                UNIQUE(guild_id, date)
            )''',

            '''CREATE TABLE IF NOT EXISTS channel_stats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                channel_id INTEGER NOT NULL,
                guild_id INTEGER NOT NULL,
                date DATE NOT NULL,
                total_messages INTEGER DEFAULT 0,
                unique_users INTEGER DEFAULT 0,
                avg_words_per_message REAL DEFAULT 0,
                UNIQUE(channel_id, date)
            )''',

            '''CREATE TABLE IF NOT EXISTS user_achievements (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                achievement_name TEXT NOT NULL,
                unlocked_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                description TEXT,
                icon TEXT DEFAULT '🏆'
            )''',

            '''CREATE TABLE IF NOT EXISTS active_voice_sessions (
                user_id INTEGER PRIMARY KEY,
                guild_id INTEGER NOT NULL,
                channel_id INTEGER NOT NULL,
                start_time DATETIME DEFAULT CURRENT_TIMESTAMP
            )'''
        ]

        for table_sql in tables:
            self.cursor.execute(table_sql)

        # Migration: Add is_private if not exists
        try:
            self.cursor.execute('ALTER TABLE global_user_levels ADD COLUMN is_private BOOLEAN DEFAULT 0')
            self.conn.commit()
        except sqlite3.OperationalError:
            pass # Already exists

        # Indexes for performance
        indexes = [
            'CREATE INDEX IF NOT EXISTS idx_messages_user_timestamp ON messages(user_id, timestamp)',
            'CREATE INDEX IF NOT EXISTS idx_messages_guild_timestamp ON messages(guild_id, timestamp)',
            'CREATE INDEX IF NOT EXISTS idx_voice_user_timestamp ON voice_sessions(user_id, start_time)',
            'DROP INDEX IF EXISTS idx_daily_stats_guild_date',
            'CREATE UNIQUE INDEX IF NOT EXISTS idx_daily_stats_unique ON daily_stats(guild_id, date)',
            'CREATE UNIQUE INDEX IF NOT EXISTS idx_channel_stats_unique ON channel_stats(channel_id, date)',
            'CREATE INDEX IF NOT EXISTS idx_global_levels_xp ON global_user_levels(global_xp DESC)'
        ]

        for index_sql in indexes:
            self.cursor.execute(index_sql)

        self.conn.commit()
        logger.info("Privacy-First Stats database initialized")

    # ------------------------------------------------------------------
    # Write Operations
    # ------------------------------------------------------------------

    async def log_message(self, user_id: int, guild_id: int, channel_id: int, message_id: int,
                          word_count: int = 0, has_attachment: bool = False, message_type: str = 'text'):
        """Log a message and update global XP."""
        async with self.lock:
            try:
                # Raw event (user-bound, cleaned up after 30 days / monthly reset)
                self.cursor.execute('''
                    INSERT INTO messages (user_id, guild_id, channel_id, message_id, word_count, has_attachment, message_type)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (user_id, guild_id, channel_id, message_id, word_count, has_attachment, message_type))

                # Anonymized server-level daily aggregate
                today = datetime.now().date()
                self.cursor.execute('''
                    INSERT INTO daily_stats (guild_id, date, messages_count)
                    VALUES (?, ?, 1)
                    ON CONFLICT(guild_id, date) DO UPDATE SET messages_count = messages_count + 1
                ''', (guild_id, today))

                # Update global level system
                await self._update_global_xp(user_id, guild_id, 'message', word_count)

                self.conn.commit()

            except Exception as e:
                logger.error(f"Error logging message: {e}")
                self.conn.rollback()

    async def start_voice_session(self, user_id: int, guild_id: int, channel_id: int):
        """Start a voice session."""
        async with self.lock:
            try:
                await self._end_existing_voice_session(user_id)

                self.cursor.execute('''
                    INSERT INTO active_voice_sessions (user_id, guild_id, channel_id)
                    VALUES (?, ?, ?)
                ''', (user_id, guild_id, channel_id))

                self.conn.commit()

            except Exception as e:
                logger.error(f"Error starting voice session: {e}")
                self.conn.rollback()

    async def end_voice_session(self, user_id: int, channel_id: int):
        """End a voice session and calculate duration."""
        async with self.lock:
            try:
                self.cursor.execute('''
                    SELECT guild_id, channel_id, start_time FROM active_voice_sessions
                    WHERE user_id = ?
                ''', (user_id,))

                session = self.cursor.fetchone()
                if not session:
                    return

                guild_id, session_channel_id, start_time = session
                start_datetime = datetime.fromisoformat(start_time)
                duration_minutes = (datetime.now() - start_datetime).total_seconds() / 60
                if duration_minutes > 1440:  # 24 Stunden Cap (Anti-Anomalie)
                    duration_minutes = 1440

                if duration_minutes > 0.5:
                    # Raw session (user-bound, cleaned up after 30 days / monthly reset)
                    self.cursor.execute('''
                        INSERT INTO voice_sessions (user_id, guild_id, channel_id, start_time, end_time, duration_minutes)
                        VALUES (?, ?, ?, ?, ?, ?)
                    ''', (user_id, guild_id, session_channel_id, start_time, datetime.now(), duration_minutes))

                    # Anonymized server-level daily aggregate
                    today = datetime.now().date()
                    self.cursor.execute('''
                        INSERT INTO daily_stats (guild_id, date, voice_minutes)
                        VALUES (?, ?, ?)
                        ON CONFLICT(guild_id, date) DO UPDATE SET voice_minutes = voice_minutes + ?
                    ''', (guild_id, today, duration_minutes, duration_minutes))

                    # Update global XP
                    await self._update_global_xp(user_id, guild_id, 'voice', duration_minutes)

                self.cursor.execute('DELETE FROM active_voice_sessions WHERE user_id = ?', (user_id,))
                self.conn.commit()

            except Exception as e:
                logger.error(f"Error ending voice session: {e}")
                self.conn.rollback()

    async def _end_existing_voice_session(self, user_id: int):
        """Helper to end any existing voice session."""
        self.cursor.execute('SELECT channel_id FROM active_voice_sessions WHERE user_id = ?', (user_id,))
        existing = self.cursor.fetchone()
        if existing:
            await self.end_voice_session(user_id, existing[0])

    # ------------------------------------------------------------------
    # Global XP & Levels
    # ------------------------------------------------------------------

    async def _update_global_xp(self, user_id: int, guild_id: int, activity_type: str, value: float = 0):
        """Update global XP and level system."""
        try:
            xp_gain = 0
            if activity_type == 'message':
                base_xp = 1
                word_bonus = min(value * 0.1, 5)
                xp_gain = base_xp + word_bonus
            elif activity_type == 'voice':
                xp_gain = value * 0.5  # 0.5 XP per minute

            self.cursor.execute('''
                SELECT global_level, global_xp, total_messages, total_voice_minutes, total_servers,
                       last_daily_activity, daily_streak
                FROM global_user_levels WHERE user_id = ?
            ''', (user_id,))

            user_data = self.cursor.fetchone()
            today = datetime.now().date()

            if user_data:
                current_level, current_xp, total_msg, total_voice, total_servers, last_daily, daily_streak = user_data

                if last_daily:
                    last_date = datetime.strptime(last_daily, '%Y-%m-%d').date()
                    if today == last_date + timedelta(days=1):
                        daily_streak += 1
                    elif today != last_date:
                        daily_streak = 1
                else:
                    daily_streak = 1

                new_xp = current_xp + xp_gain
                new_level = self._calculate_level(new_xp)

                if activity_type == 'message':
                    total_msg += 1
                elif activity_type == 'voice':
                    total_voice += value

                self.cursor.execute(
                    'SELECT COUNT(DISTINCT guild_id) FROM messages WHERE user_id = ?', (user_id,)
                )
                server_count = self.cursor.fetchone()[0] or 1

                self.cursor.execute('''
                    UPDATE global_user_levels
                    SET global_level = ?, global_xp = ?, total_messages = ?, total_voice_minutes = ?,
                        total_servers = ?, last_activity = ?, last_daily_activity = ?, daily_streak = ?,
                        best_streak = MAX(best_streak, ?)
                    WHERE user_id = ?
                ''', (new_level, new_xp, total_msg, total_voice, server_count, datetime.now(),
                      today, daily_streak, daily_streak, user_id))

                if new_level > current_level:
                    await self._check_level_achievements(user_id, new_level)

            else:
                initial_level = self._calculate_level(xp_gain)
                self.cursor.execute('''
                    INSERT INTO global_user_levels
                    (user_id, global_level, global_xp, total_messages, total_voice_minutes, total_servers,
                     last_daily_activity, daily_streak, best_streak)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (user_id, initial_level, xp_gain,
                      1 if activity_type == 'message' else 0,
                      value if activity_type == 'voice' else 0,
                      1, today, 1, 1))

        except Exception as e:
            logger.error(f"Error updating global XP: {e}")

    def _calculate_level(self, xp: float) -> int:
        """Level formula: XP = 50 * (Level-1)^1.5 -> Level = (XP/50)^(1/1.5) + 1"""
        if xp < 50:
            return 1
        # 1/1.5 = 2/3 approx 0.666
        return int((xp / 50) ** (2/3)) + 1

    def _xp_for_level(self, level: int) -> int:
        """XP required to reach a specific level: 50 * (Level-1)^1.5"""
        if level <= 1:
            return 0
        return int(50 * ((level - 1) ** 1.5))

    # ------------------------------------------------------------------
    # Read Operations
    # ------------------------------------------------------------------

    async def get_user_stats(self, user_id: int, hours: int = 24, guild_id: Optional[int] = None) -> Tuple[int, float]:
        """Get user statistics for a time period (reads from raw event tables)."""
        async with self.lock:
            try:
                cutoff_time = datetime.now() - timedelta(hours=hours)

                if guild_id:
                    self.cursor.execute('''
                        SELECT COUNT(*) FROM messages
                        WHERE user_id = ? AND guild_id = ? AND timestamp > ?
                    ''', (user_id, guild_id, cutoff_time))
                else:
                    self.cursor.execute('''
                        SELECT COUNT(*) FROM messages
                        WHERE user_id = ? AND timestamp > ?
                    ''', (user_id, cutoff_time))

                message_count = self.cursor.fetchone()[0] or 0

                if guild_id:
                    self.cursor.execute('''
                        SELECT COALESCE(SUM(duration_minutes), 0) FROM voice_sessions
                        WHERE user_id = ? AND guild_id = ? AND start_time > ?
                    ''', (user_id, guild_id, cutoff_time))
                else:
                    self.cursor.execute('''
                        SELECT COALESCE(SUM(duration_minutes), 0) FROM voice_sessions
                        WHERE user_id = ? AND start_time > ?
                    ''', (user_id, cutoff_time))

                voice_minutes = self.cursor.fetchone()[0] or 0
                return message_count, voice_minutes

            except Exception as e:
                logger.error(f"Error getting user stats: {e}")
                return 0, 0

    async def get_global_user_info(self, user_id: int) -> Optional[Dict]:
        """Get global user information including level and achievements."""
        async with self.lock:
            try:
                self.cursor.execute('''
                    SELECT global_level, global_xp, total_messages, total_voice_minutes, total_servers,
                           daily_streak, best_streak, first_seen, achievements, is_private
                    FROM global_user_levels WHERE user_id = ?
                ''', (user_id,))

                result = self.cursor.fetchone()
                if not result:
                    return None

                level, xp, total_msg, total_voice, servers, streak, best_streak, first_seen, achievements, is_private = result

                # Rank berechnen
                self.cursor.execute("SELECT COUNT(*) + 1 FROM global_user_levels WHERE global_xp > ?", (xp,))
                rank = self.cursor.fetchone()[0]

                next_level_xp = self._xp_for_level(level + 1)
                current_level_xp = self._xp_for_level(level)
                xp_progress = xp - current_level_xp
                xp_needed = next_level_xp - current_level_xp

                return {
                    'level': level,
                    'xp': xp,
                    'xp_progress': xp_progress,
                    'xp_needed': xp_needed,
                    'total_messages': total_msg,
                    'total_voice_minutes': total_voice,
                    'total_servers': servers,
                    'daily_streak': streak,
                    'best_streak': best_streak,
                    'first_seen': first_seen,
                    'achievements': json.loads(achievements) if achievements else [],
                    'is_private': is_private,
                    'rank': rank
                }

            except Exception as e:
                logger.error(f"Error getting global user info: {e}")
                return None

    async def get_leaderboard(self, limit: int = 10,
                              guild_id: Optional[int] = None,
                              bot=None) -> List[Tuple]:
        """
        Get global or guild-specific leaderboard.

        If `bot` is provided, each user_id is validated via bot.get_user().
        Entries for users that can no longer be resolved are hard-deleted
        immediately so they never appear as 'Unknown User' again.
        """
        async with self.lock:
            try:
                if guild_id:
                    self.cursor.execute('''
                        SELECT user_id, COUNT(*) as messages,
                               COALESCE(SUM(word_count), 0) as total_words
                        FROM messages
                        WHERE guild_id = ? AND timestamp > datetime('now', '-30 days')
                        GROUP BY user_id
                        ORDER BY messages DESC
                        LIMIT ?
                    ''', (guild_id, limit))
                else:
                    self.cursor.execute('''
                        SELECT user_id, global_level, global_xp, total_messages, total_voice_minutes, is_private
                        FROM global_user_levels
                        ORDER BY global_xp DESC
                        LIMIT ?
                    ''', (limit,))

                rows = self.cursor.fetchall()

                if bot is None:
                    return rows

                # --- Orphan cleanup ---
                clean_rows = []
                orphan_ids = []

                for row in rows:
                    uid = row[0]
                    if bot.get_user(uid) is None:
                        orphan_ids.append(uid)
                    else:
                        clean_rows.append(row)

                if orphan_ids:
                    for uid in orphan_ids:
                        self._hard_delete_user(uid)
                    self.conn.commit()
                    logger.info(f"Leaderboard cleanup: removed {len(orphan_ids)} orphan user(s): {orphan_ids}")

                return clean_rows

            except Exception as e:
                logger.error(f"Error getting leaderboard: {e}")
                return []

    # ------------------------------------------------------------------
    # Privacy & Maintenance
    # ------------------------------------------------------------------

    async def monthly_season_reset(self):
        """
        Season reset: wipe messages and voice_sessions on the 1st of each month.
        daily_stats is also wiped since it reflects the season window.
        global_user_levels are reset (XP, Level, Totals) but achievements remain.
        """
        today = datetime.now()
        if today.day != 1:
            logger.debug("monthly_season_reset: not the 1st of the month, skipping.")
            return

        async with self.lock:
            try:
                # Clear raw event tables
                self.cursor.execute('DELETE FROM messages')
                self.cursor.execute('DELETE FROM voice_sessions')
                self.cursor.execute('DELETE FROM daily_stats')
                
                # Reset global level stats for all users (preserve user_id, first_seen and achievements)
                self.cursor.execute('''
                    UPDATE global_user_levels
                    SET global_level = 1, 
                        global_xp = 0, 
                        total_messages = 0, 
                        total_voice_minutes = 0, 
                        total_servers = 0, 
                        daily_streak = 0, 
                        best_streak = 0,
                        last_daily_activity = NULL
                ''')
                
                self.conn.commit()
                logger.info(
                    f"[Season Reset] Monthly wipe completed on {today.strftime('%Y-%m-%d')}. "
                    "All levels, XP and activity stats have been reset."
                )
            except Exception as e:
                logger.error(f"Error during monthly season reset: {e}")
                self.conn.rollback()

    async def cleanup_old_data(self, days: int = 30):
        """
        Rolling cleanup: delete raw event data older than `days` days.
        Default changed from 90 to 30 (Privacy-First).
        """
        async with self.lock:
            try:
                cutoff_date = datetime.now() - timedelta(days=days)

                self.cursor.execute('DELETE FROM messages WHERE timestamp < ?', (cutoff_date,))
                self.cursor.execute('DELETE FROM voice_sessions WHERE start_time < ?', (cutoff_date,))
                # daily_stats is anonymous but we trim it too for hygiene
                self.cursor.execute('DELETE FROM daily_stats WHERE date < ?', (cutoff_date.date(),))

                self.conn.commit()
                logger.info(f"Rolling cleanup: removed data older than {days} days")

            except Exception as e:
                logger.error(f"Error cleaning up old data: {e}")

    async def delete_user_data(self, user_id: int) -> bool:
        """
        Hard Delete – removes ALL data for a user across every table.
        Called by /user data delete. Returns True on success, False on error.
        """
        async with self.lock:
            try:
                self._hard_delete_user(user_id)
                self.conn.commit()
                logger.info(f"Hard delete completed for user_id={user_id}")
                return True
            except Exception as e:
                logger.error(f"Error during hard delete for user_id={user_id}: {e}")
                self.conn.rollback()
                return False

    def _hard_delete_user(self, user_id: int):
        """
        Synchronous inner helper that deletes a user from all tables.
        Must be called inside an existing lock context.
        """
        self.cursor.execute('DELETE FROM messages WHERE user_id = ?', (user_id,))
        self.cursor.execute('DELETE FROM voice_sessions WHERE user_id = ?', (user_id,))
        self.cursor.execute('DELETE FROM active_voice_sessions WHERE user_id = ?', (user_id,))
        self.cursor.execute('DELETE FROM global_user_levels WHERE user_id = ?', (user_id,))
        self.cursor.execute('DELETE FROM user_achievements WHERE user_id = ?', (user_id,))

    # ------------------------------------------------------------------
    # Achievements
    # ------------------------------------------------------------------

    async def _check_level_achievements(self, user_id: int, new_level: int):
        """Check and award level-based achievements."""
        level_milestones = {
            5:   ("Newcomer", "Reached level 5!",   "🌟"),
            10:  ("Regular",  "Reached level 10!",  "⭐"),
            25:  ("Veteran",  "Reached level 25!",  "🏅"),
            50:  ("Expert",   "Reached level 50!",  "🏆"),
            100: ("Legend",   "Reached level 100!", "👑"),
        }

        for milestone, (name, desc, icon) in level_milestones.items():
            if new_level >= milestone:
                self.cursor.execute('''
                    SELECT id FROM user_achievements
                    WHERE user_id = ? AND achievement_name = ?
                ''', (user_id, name))

                if not self.cursor.fetchone():
                    self.cursor.execute('''
                        INSERT INTO user_achievements (user_id, achievement_name, description, icon)
                        VALUES (?, ?, ?, ?)
                    ''', (user_id, name, desc, icon))

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def close(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()
            logger.info("Privacy-First Stats database connection closed")

    async def get_daily_messages(self, guild_id: int, date: str) -> int:
        """Get message count for a specific date."""
        async with self.lock:
            try:
                self.cursor.execute('SELECT messages_count FROM daily_stats WHERE guild_id = ? AND date = ?', (guild_id, date))
                row = self.cursor.fetchone()
                return row[0] if row else 0
            except Exception as e:
                logger.error(f"Error getting daily messages: {e}")
                return 0

    async def get_weekly_stats(self, guild_id: int) -> list:
        """Get message stats for the last 7 days."""
        async with self.lock:
            try:
                self.cursor.execute('''
                    SELECT date, messages_count FROM daily_stats 
                    WHERE guild_id = ? AND date > date('now', '-7 days')
                    ORDER BY date ASC
                ''', (guild_id,))
                rows = self.cursor.fetchall()
                return [{"date": row[0], "messages": row[1]} for row in rows]
            except Exception as e:
                logger.error(f"Error getting weekly stats: {e}")
                return []
