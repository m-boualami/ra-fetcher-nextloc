# ra-fetcher-nextloc

> ⚠️ **In Development**

A scraper and data fetcher for Resident Advisor with two core functionalities:

| # | Functionality | Source |
|---|--------------|--------|
| 1 | Fetch artist touring info | `./src_nextloc/` |
| 2 | Fetch event metadata worldwide | `./src/4u2/` |

Specifically, this tool fetches:
- Artist IDs that played in a given city
- Full touring history from a list of artist IDs
- City info from event IDs

## Installation
```bash
pip install -r requirements.txt
```

## Usage

See `terminal_commands.txt` for full usage instructions.

## Credits

Built on top of [resident-advisor-events-scraper](https://github.com/djb-gt/resident-advisor-events-scraper).
