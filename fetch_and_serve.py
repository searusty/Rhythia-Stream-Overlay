#!/usr/bin/env python3
"""
Rhythia Stream Overlay server and data fetcher.

This script fetches data from the Rhythia API at a configurable interval and
serves both a static overlay web page and a JSON endpoint containing the
latest player statistics. It can be run locally and integrated into
streaming software as a browser source.

Usage example:

    python fetch_and_serve.py --username searust --flag HU --interval 30

This will start a small HTTP server on http://localhost:8000 that serves
the overlay page and periodically updates the data.json file with the latest
ranking information.

You can customise the username, country flag, update interval, listening
host/port, and whether to attempt to determine the user ID automatically
by scanning the leaderboards.

"""

import argparse
import json
import math
import os
import threading
import time
from http import server
from socketserver import ThreadingMixIn
from typing import Any, Dict, Optional, Tuple

import requests
import subprocess


API_BASE = "https://production.rhythia.com/api"

# Global configuration dictionary. This will be loaded from config.json and
# updated via the /save_config endpoint at runtime.
CONFIG: Dict[str, Any] = {}

# Path to the configuration file relative to the script directory.
CONFIG_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'config.json')

# Lock to synchronise access to CONFIG
CONFIG_LOCK = threading.Lock()


def load_config() -> Dict[str, Any]:
    """Load the configuration from the CONFIG_PATH JSON file."""
    try:
        with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
            cfg = json.load(f)
            return cfg
    except Exception:
        return {}


def save_config(cfg: Dict[str, Any]) -> None:
    """Save the provided configuration dictionary to CONFIG_PATH."""
    with open(CONFIG_PATH, 'w', encoding='utf-8') as f:
        json.dump(cfg, f, ensure_ascii=False, indent=2)


def update_config(new_cfg: Dict[str, Any]) -> None:
    """
    Update the global CONFIG with new values and save to disk.

    This helper applies the provided dictionary on top of the existing
    configuration and persists the result. It acquires a lock to
    serialise concurrent updates from the settings page and the CLI.
    """
    with CONFIG_LOCK:
        CONFIG.update(new_cfg)
        save_config(CONFIG)



class RhythiaClient:
    """Client for accessing Rhythia API endpoints relevant to the overlay."""

    def __init__(self, flag: str, user_id: Optional[int] = None, username: Optional[str] = None, verbose: bool = False):
        # Flag (country code) for leaderboard filtering
        self.flag = flag
        # Numeric user ID for unique profile lookup
        self.user_id = user_id
        # Username fallback if no ID is provided (not recommended)
        self.username = username
        # Whether to emit verbose debug logs during API calls and data building
        self.verbose = verbose
        # Common headers used for all API requests
        self.headers = {
            "Content-Type": "text/plain;charset=UTF-8",
            "Origin": "https://www.rhythia.com",
            "Referer": "https://www.rhythia.com/",
            "User-Agent": "Mozilla/5.0",
            "Accept": "*/*",
        }

    def _post(self, endpoint: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Low-level POST helper. Attempts to use requests first, falling back
        to a curl-based implementation if the server responds with an
        unsupported compression encoding (e.g. zstd). This workaround
        mirrors the behaviour seen when calling the API manually via curl.
        """
        url = f"{API_BASE}/{endpoint}"
        # First try using requests. If the server responds with a
        # compression algorithm that requests can't decode (e.g. zstd),
        # requests will raise an exception or return undecoded binary data.
        try:
            resp = requests.post(url, data=json.dumps(payload), headers=self.headers, timeout=10)
            resp.raise_for_status()
            # Attempt to parse JSON; if zstd encoding is unsupported this may fail
            return resp.json()
        except Exception:
            # Fallback to curl with --compressed to handle zstd
            try:
                curl_cmd = [
                    'curl', '-sS', '--compressed', '-X', 'POST', url,
                    '-H', 'content-type: text/plain;charset=UTF-8',
                    '-H', 'origin: https://www.rhythia.com',
                    '-H', 'referer: https://www.rhythia.com/',
                    '-H', 'user-agent: Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36',
                    '--data-raw', json.dumps(payload),
                ]
                proc = subprocess.run(curl_cmd, capture_output=True, text=True, timeout=15)
                if proc.returncode != 0:
                    raise Exception(f"curl exited with code {proc.returncode}: {proc.stderr.strip()}")
                try:
                    return json.loads(proc.stdout)
                except Exception as parse_exc:
                    raise Exception(f"Failed to parse JSON from curl output: {parse_exc}; output snippet: {proc.stdout[:200]}")
            except Exception as curl_exc:
                # Propagate the curl exception up
                raise curl_exc

    def get_leaderboard_page(self, page: int) -> Dict[str, Any]:
        payload = {
            "page": page,
            "session": "",
            "flag": self.flag,
            "spin": False,
            "include_inactive": False,
        }
        return self._post("getLeaderboard", payload)

    def get_profile(self, user_id: int) -> Dict[str, Any]:
        payload = {
            "id": user_id,
            "session": "",
        }
        return self._post("getProfile", payload).get("user", {})

    def find_player(self) -> Tuple[Optional[int], Optional[Dict[str, Any]], Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
        """
        Scan the leaderboards for the player matching either the provided user ID or username.

        Returns a tuple of (rank, player_entry, above_entry, below_entry).
        If the player isn't found, returns (None, None, None, None).
        """
        # Fetch the first page to determine total pages and per-page size
        try:
            first_page = self.get_leaderboard_page(1)
        except Exception:
            return None, None, None, None
        total = first_page.get("total", 0)
        per_page = first_page.get("viewPerPage", 50) or 50
        total_pages = max(1, math.ceil(total / per_page))
        pages = [first_page]
        # Scan subsequent pages on demand
        for page_num in range(2, total_pages + 1):
            try:
                pages.append(self.get_leaderboard_page(page_num))
            except Exception:
                break
        # Flatten players and find the matching entry
        flat_players = []
        for p in pages:
            flat_players.extend(p.get("leaderboard", []))
        for idx, player in enumerate(flat_players):
            # Prefer matching by user ID if provided
            if self.user_id is not None and int(player.get("id", -1)) == int(self.user_id):
                rank = idx + 1
                above = flat_players[idx - 1] if idx > 0 else None
                below = flat_players[idx + 1] if idx + 1 < len(flat_players) else None
                return rank, player, above, below
            # Fallback to username matching (discouraged)
            if self.username and player.get("username", "").lower() == self.username.lower():
                rank = idx + 1
                above = flat_players[idx - 1] if idx > 0 else None
                below = flat_players[idx + 1] if idx + 1 < len(flat_players) else None
                return rank, player, above, below
        return None, None, None, None

    def build_data(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Fetch the latest player stats based on the current configuration and
        construct a dictionary for output to data.json. This implementation
        prioritises fetching the user's profile directly by ID to avoid
        reliance on leaderboard scanning. If the profile is available, it
        populates basic fields (username, flag, skill points, plays, avatar,
        global/country rank, since date, title info, rhythm points). If the
        profile is unavailable and a username and flag are provided, a
        fallback leaderboard scan is attempted. Gap calculations are
        performed only if explicitly requested and a flag and rank are
        available. All exceptions are logged but do not prevent data
        emission. A 'features' sub-dictionary and 'displayMode' value are
        always attached to the returned data.
        """
        # Update cached parameters from config
        self.flag = config.get('flag', self.flag)
        self.user_id = config.get('user_id', self.user_id)
        self.username = config.get('username', self.username)

        data: Dict[str, Any] = {}
        uid: Optional[int] = None
        if self.user_id:
            try:
                uid = int(self.user_id)
            except Exception:
                uid = None
        # Step 1: attempt to fetch profile
        profile: Dict[str, Any] = {}
        if uid is not None:
            try:
                if self.verbose:
                    print(f"[overlay] Fetching profile for user_id {uid}")
                profile = self.get_profile(uid)
                if self.verbose:
                    try:
                        # Log a small snippet of the profile for visibility
                        snippet = json.dumps(profile)[:300]
                        print(f"[overlay] Profile data snippet: {snippet}...")
                    except Exception:
                        pass
            except Exception as e:
                if self.verbose:
                    print(f"[overlay] Failed to fetch profile: {e}")
                profile = {}
        # Step 2: fallback scan if no profile but have username and flag
        player_entry: Optional[Dict[str, Any]] = None
        rank: Optional[int] = None
        above: Optional[Dict[str, Any]] = None
        below: Optional[Dict[str, Any]] = None
        if not profile:
            scan_flag = config.get('flag') or self.flag
            if self.username and scan_flag:
                try:
                    if self.verbose:
                        print(f"[overlay] Scanning leaderboard for username '{self.username}' with flag '{scan_flag}'")
                    rank, player_entry, above, below = self.find_player()
                    if rank is not None and player_entry is not None and self.verbose:
                        print(f"[overlay] Found player via leaderboard at rank {rank}")
                except Exception as scan_exc:
                    if self.verbose:
                        print(f"[overlay] Failed to scan leaderboard: {scan_exc}")
        # Step 3: populate data from profile or leaderboard
        if profile:
            data['username'] = profile.get('username') or profile.get('name')
            # Use the flag from profile if present, else cached flag
            prof_flag = profile.get('flag')
            if prof_flag:
                data['flag'] = prof_flag
                self.flag = prof_flag
            else:
                data['flag'] = self.flag
            # Skill points and play count
            try:
                data['skill_points'] = float(profile.get('skill_points', 0))
            except Exception:
                data['skill_points'] = 0.0
            try:
                data['play_count'] = int(profile.get('play_count', 0))
            except Exception:
                data['play_count'] = 0
            # Avatar
            data['avatar_url'] = profile.get('avatar_url') or profile.get('profile_image') or ''
            # Global and country ranks
            try:
                data['global_rank'] = int(profile.get('position')) if profile.get('position') is not None else None
            except Exception:
                data['global_rank'] = None
            try:
                data['country_rank'] = int(profile.get('country_position')) if profile.get('country_position') is not None else None
            except Exception:
                data['country_rank'] = None
            # Determine rank for display: prefer country rank
            if data.get('country_rank'):
                data['rank'] = data['country_rank']
            elif data.get('global_rank'):
                data['rank'] = data['global_rank']
            else:
                data['rank'] = None
            # Since date
            created_ts = profile.get('created_at')
            if created_ts:
                try:
                    created_sec = int(created_ts) / 1000.0
                    data['since'] = time.strftime('%Y %b %d', time.localtime(created_sec))
                except Exception:
                    data['since'] = None
            else:
                data['since'] = None
            # Title determination based on skill points
            sp = data.get('skill_points', 0) or 0
            title_name: Optional[str] = None
            title_icon: Optional[str] = None
            try:
                if sp >= 10000:
                    title_name = 'Grandmaster'
                    title_icon = 'https://www.rhythia.com/titles/grandmaster.png'
                elif sp >= 5000:
                    title_name = 'Master'
                    title_icon = 'https://www.rhythia.com/titles/master.png'
                elif sp >= 3000:
                    title_name = 'Candidate Master'
                    title_icon = 'https://www.rhythia.com/titles/candidate-master.png'
                elif sp >= 1500:
                    title_name = 'Expert'
                    title_icon = 'https://www.rhythia.com/titles/expert.png'
                else:
                    title_name = 'Novice'
                    title_icon = 'https://www.rhythia.com/titles/novice.png'
            except Exception:
                title_name = None
                title_icon = None
            data['title_name'] = title_name
            data['title_icon'] = title_icon
            data['rhythm_points'] = data['skill_points']
        elif player_entry:
            # Use leaderboard entry if profile isn't available
            data['username'] = player_entry.get('username')
            data['flag'] = player_entry.get('flag', self.flag)
            data['rank'] = rank
            try:
                data['skill_points'] = float(player_entry.get('skill_points', 0))
            except Exception:
                data['skill_points'] = 0.0
            try:
                data['play_count'] = int(player_entry.get('play_count', 0))
            except Exception:
                data['play_count'] = 0
            data['avatar_url'] = player_entry.get('profile_image') or player_entry.get('avatar_url') or ''
            data['global_rank'] = None
            data['country_rank'] = None
            data['since'] = None
            data['title_name'] = None
            data['title_icon'] = None
            data['rhythm_points'] = data['skill_points']
        else:
            # Neither profile nor leaderboard found
            if self.verbose:
                print("[overlay] No profile or leaderboard data available")
            return data
        # Step 4: compute gaps if requested and possible
        data['gap_up'] = None
        data['gap_down'] = None
        try:
            needs_gap_up = config.get('showGapUp', True)
            needs_gap_down = config.get('showGapDown', True)
            if (needs_gap_up or needs_gap_down) and self.flag and data.get('rank'):
                if self.verbose:
                    print(f"[overlay] Attempting gap calculation for flag '{self.flag}'")
                # Ensure flag is correctly set on the client
                self.flag = self.flag or data.get('flag')
                rnk, entry, above, below = self.find_player()
                if rnk and entry:
                    if needs_gap_up and above:
                        try:
                            gap_up_val = float(above.get('skill_points', 0)) - data['skill_points']
                            data['gap_up'] = max(gap_up_val, 0)
                        except Exception:
                            data['gap_up'] = None
                    if needs_gap_down and below:
                        try:
                            gap_down_val = data['skill_points'] - float(below.get('skill_points', 0))
                            data['gap_down'] = max(gap_down_val, 0)
                        except Exception:
                            data['gap_down'] = None
        except Exception as gap_exc:
            if self.verbose:
                print(f"[overlay] Gap calculation failed: {gap_exc}")
            data['gap_up'] = None
            data['gap_down'] = None
        # Step 5: append feature toggles and display mode
        features = {
            'showAvatar': bool(config.get('showAvatar', True)),
            'showFlag': bool(config.get('showFlag', True)),
            'showUsername': bool(config.get('showUsername', True)),
            'showRank': bool(config.get('showRank', True)),
            'showSkillPoints': bool(config.get('showSkillPoints', True)),
            'showPlays': bool(config.get('showPlays', True)),
            'showGapUp': bool(config.get('showGapUp', True)),
            'showGapDown': bool(config.get('showGapDown', True)),
            'showSince': bool(config.get('showSince', True)),
            'showTitle': bool(config.get('showTitle', True)),
            'showGlobalRank': bool(config.get('showGlobalRank', True)),
            'showCountryRank': bool(config.get('showCountryRank', True)),
            'showRhythmPoints': bool(config.get('showRhythmPoints', True)),
        }
        data['features'] = features
        data['displayMode'] = config.get('displayMode', 'minimal')
        return data


class OverlayRequestHandler(server.SimpleHTTPRequestHandler):
    """Custom handler to serve overlay files, configuration and data with no caching."""

    def end_headers(self) -> None:
        # Prevent browser caching of dynamic content
        self.send_header('Cache-Control', 'no-store, no-cache, must-revalidate')
        super().end_headers()

    def do_GET(self) -> None:
        """Intercept specific endpoints for configuration and data before serving static files."""
        if self.path.startswith('/config.json'):
            with CONFIG_LOCK:
                cfg = CONFIG.copy()
            body = json.dumps(cfg).encode('utf-8')
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        if self.path.startswith('/data.json'):
            # Return the latest data.json file contents
            data_path = os.path.join(os.getcwd(), 'data.json')
            if os.path.exists(data_path):
                try:
                    with open(data_path, 'rb') as f:
                        body = f.read()
                    self.send_response(200)
                    self.send_header('Content-Type', 'application/json')
                    self.send_header('Content-Length', str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                except Exception:
                    self.send_error(500, 'Could not read data file')
            else:
                self.send_error(404, 'Data file not found')
            return
        # For other paths, fall back to static file handling
        super().do_GET()

    def do_POST(self) -> None:
        """Handle configuration updates via POST to /save_config."""
        if self.path.startswith('/save_config'):
            try:
                # Read and parse the JSON body
                content_length = int(self.headers.get('Content-Length', 0))
                body = self.rfile.read(content_length)
                cfg_update = json.loads(body.decode('utf-8'))
                # Validate required fields – user_id and interval must be present
                if 'user_id' not in cfg_update or 'interval' not in cfg_update:
                    raise ValueError('Missing required fields (user_id and interval are required)')
                # Normalise numeric values
                cfg_update['user_id'] = int(cfg_update['user_id'])
                cfg_update['interval'] = max(1, int(cfg_update['interval']))
                # Normalise displayMode if provided, default to 'minimal'
                display_mode = cfg_update.get('displayMode', CONFIG.get('displayMode', 'minimal'))
                if isinstance(display_mode, str):
                    display_mode = display_mode.lower()
                    if display_mode not in ['minimal', 'maximal']:
                        display_mode = 'minimal'
                else:
                    display_mode = 'minimal'
                cfg_update['displayMode'] = display_mode
                # Set default booleans if not provided; include new feature toggles
                booleans = [
                    'showAvatar','showFlag','showUsername','showRank','showSkillPoints',
                    'showPlays','showGapUp','showGapDown','showSince','showTitle',
                    'showGlobalRank','showCountryRank','showRhythmPoints'
                ]
                for key in booleans:
                    cfg_update[key] = bool(cfg_update.get(key, True))
                update_config(cfg_update)
                self.send_response(200)
                self.end_headers()
                self.wfile.write(b'OK')
            except Exception as e:
                self.send_response(400)
                self.end_headers()
                self.wfile.write(f'Bad Request: {e}'.encode('utf-8'))
            return
        # Other POST requests are not supported
        self.send_error(404, 'Not Found')

    # Suppress default HTTP logging for each request. The base class
    # writes a line for every GET/POST to stderr. Overriding this
    # prevents the server from spamming logs with every /data.json request.
    def log_message(self, format: str, *args: Any) -> None:  # type: ignore[override]
        # Only log messages when verbose mode is enabled on the client.
        # We look up a global verbose flag attached to the RhythiaClient instance.
        try:
            # Access the bound RhythiaClient via CONFIG or defaults
            verbose = False
            # No prints by default
            if verbose:
                super().log_message(format, *args)
        except Exception:
            pass


class ThreadedHTTPServer(ThreadingMixIn, server.HTTPServer):
    daemon_threads = True


def start_server(directory: str, host: str = '0.0.0.0', port: int = 8000) -> server.HTTPServer:
    """
    Start a threaded HTTP server serving files from the given directory.
    Returns the server instance so it can be shut down if needed.
    """
    os.chdir(directory)
    httpd = ThreadedHTTPServer((host, port), OverlayRequestHandler)
    server_thread = threading.Thread(target=httpd.serve_forever)
    server_thread.daemon = True
    server_thread.start()
    return httpd


def data_fetch_loop(client: RhythiaClient, data_path: str, verbose: bool = False) -> None:
    """
    Periodically fetch player data based on the current configuration and write it
    to the specified JSON file. Reads the config before each iteration so
    changes take effect without restarting the loop.

    If verbose is True, extra details will be printed to the console to aid
    debugging, including the full response dictionary.
    """
    # Keep a record of the last summary values logged. We compare against
    # these values to avoid spamming the console when nothing changes.
    prev_summary: Tuple[Any, Any, Any, Any, Any] = (None, None, None, None, None)

    while True:
        try:
            # Acquire current configuration snapshot
            with CONFIG_LOCK:
                cfg = CONFIG.copy()
            # Only fetch if essential fields are present
            if not cfg.get('user_id') and not cfg.get('username'):
                data = {}
                print("[overlay] No user_id or username configured; skipping fetch.")
            else:
                data = client.build_data(cfg)
                # Determine whether to log based on verbosity and changes in summary values.
                # A summary is defined as a tuple of (rank, global_rank, country_rank, skill_points, play_count).
                current_summary: Tuple[Any, Any, Any, Any, Any] = (
                    data.get('rank'), data.get('global_rank'), data.get('country_rank'),
                    data.get('skill_points'), data.get('play_count')
                )
                if verbose:
                    try:
                        # Always log the full data if verbose
                        print(f"[overlay] Data fetched: {json.dumps(data, ensure_ascii=False)}")
                    except Exception as exc:
                        print(f"[overlay] Warning: could not dump data for logging: {exc}")
                else:
                    # Log only if the summary changed since the previous iteration
                    if current_summary != prev_summary:
                        try:
                            print(
                                f"[overlay] Updated: rank={data.get('rank')}, global_rank={data.get('global_rank')}, "
                                f"country_rank={data.get('country_rank')}, sp={data.get('skill_points')}, plays={data.get('play_count')}"
                            )
                        except Exception:
                            pass
                        prev_summary = current_summary
            # Include current interval in the output for the overlay to adjust polling
            data['interval'] = cfg.get('interval', 30)
            with open(data_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False)
        except Exception as e:
            # Write an error state to the file so the overlay can show fallback text
            error_message = str(e)
            print(f"[overlay] Error during data fetch: {error_message}")
            error_data = {"error": error_message}
            try:
                with open(data_path, 'w', encoding='utf-8') as f:
                    json.dump(error_data, f, ensure_ascii=False)
            except Exception as io_exc:
                print(f"[overlay] Additional error writing error file: {io_exc}")
        # Determine sleep interval from config (default 30 seconds)
        with CONFIG_LOCK:
            interval = int(CONFIG.get('interval', 30))
        time.sleep(max(1, interval))


def main():
    parser = argparse.ArgumentParser(description="Rhythia Stream Overlay Data Fetcher and Server")
    parser.add_argument('--username', default=None, help='Rhythia username (used only if ID is not provided)')
    # Flag input is optional; if omitted the player's flag from the profile will be used
    parser.add_argument('--flag', default=None, help='Country code (e.g. HU, US) for the leaderboard (optional)')
    parser.add_argument('--user-id', type=int, default=None, help='Numeric Rhythia user ID')
    parser.add_argument('--interval', type=int, default=None, help='Update interval in seconds (default 30)')
    parser.add_argument('--host', default='0.0.0.0', help='Host address to bind the HTTP server to')
    parser.add_argument('--port', type=int, default=8000, help='Port number for the HTTP server')
    parser.add_argument('--directory', default=os.path.dirname(os.path.realpath(__file__)), help='Directory containing overlay files and data.json')
    parser.add_argument('--verbose', action='store_true', help='Enable verbose logging for debugging')
    args = parser.parse_args()

    # Compute absolute directory path and data file location
    directory = os.path.abspath(args.directory)
    data_path = os.path.join(directory, 'data.json')

    # Load existing configuration or initialize new one
    global CONFIG
    with CONFIG_LOCK:
        CONFIG = load_config()
    # Apply CLI overrides to configuration and ensure required fields are set
    cfg_updates: Dict[str, Any] = {}
    if args.user_id is not None:
        cfg_updates['user_id'] = args.user_id
    if args.flag is not None:
        cfg_updates['flag'] = args.flag.upper()
    if args.interval is not None:
        cfg_updates['interval'] = max(1, args.interval)
    if args.username is not None:
        cfg_updates['username'] = args.username
    # Provide defaults if config is empty
    if not CONFIG and not cfg_updates:
        print('Configuration is missing. Please provide --user-id and --flag or set them via settings page.')
    if cfg_updates:
        update_config(cfg_updates)
    # After update, ensure CONFIG is loaded
    with CONFIG_LOCK:
        cfg = CONFIG.copy()
    # Instantiate client with config values
    client = RhythiaClient(flag=cfg.get('flag', ''), user_id=cfg.get('user_id'), username=cfg.get('username'), verbose=args.verbose)
    # Start background data fetch thread
    fetch_thread = threading.Thread(target=data_fetch_loop, args=(client, data_path, args.verbose), daemon=True)
    fetch_thread.start()
    # Start HTTP server
    httpd = start_server(directory=directory, host=args.host, port=args.port)
    print(f"Serving overlay on http://{args.host}:{args.port}/overlay.html")
    print(f"Settings page: http://{args.host}:{args.port}/settings.html")
    print("Press Ctrl+C to stop.")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nShutting down server...")
        httpd.shutdown()


if __name__ == '__main__':
    main()