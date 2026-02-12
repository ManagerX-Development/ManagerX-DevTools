# ManagerX DevTools Library

A comprehensive collection of development tools and database wrappers designed for the ManagerX ecosystem.

## Installation

This package is intended for internal use within the ManagerX project structure, but can be installed via pip if hosted:

```bash
pip install ManagerX-DevTools
```

## Features

This library provides easy-to-access database wrappers for various ManagerX bot modules. Imports are streamlined for cleaner code.

### Available Database Wrappers

Everything can be imported directly from `mx_devtools`:

- **AntiSpamDatabase**: Manages anti-spam configurations and data.
- **AutoDeleteDB**: Handles auto-deletion settings.
- **AutoRoleDatabase**: Manages automatic role assignments.
- **GlobalChatDatabase**: Backend for the global chat system.
- **LevelDatabase**: Handles the leveling system data.
- **LoggingDatabase**: Manages logging configurations.
- **NotesDatabase**: Stores user notes for moderation.
- **ProfileDB**: Manages user profiles.
- **SettingsDB**: General bot settings.
- **StatsDB**: Tracks user and server statistics.
- **TempVCDatabase**: Manages temporary voice channels.
- **WarnDatabase**: Stores user warnings.
- **WelcomeDatabase**: Configures welcome messages.

## Usage

### Basic Import

All database classes are exposed at the top level of the package:

```python
from mx_devtools import StatsDB, LevelDatabase, WarnDatabase
```

### Example: Using StatsDB

```python
from mx_devtools import StatsDB
import asyncio

async def get_user_stats(user_id: int):
    # Assuming StatsDB has a method to fetch stats
    stats = await StatsDB.get_stats(user_id)
    return stats
```

## Structure

The package is structured as follows:

- `mx_devtools`: Top-level package.
  - `backend`: Backend logic and database connections.
    - `database`: Contains specific database wrapper implementations for each module.

## Links

- [Homepage](https://oppro-network.de/)
- [Repository](https://github.com/ManagerX-Development/ManagerX-DevTools)
- [Documentation](https://docs.oppro-network.de/ManagerX-DevTools/)
- [Issue Tracker](https://github.com/ManagerX-Development/ManagerX-DevTools/issues)

---
Copyright (c) 2026 OPPRO.NET Development