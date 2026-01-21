#!/usr/bin/env python3 main script to run
"""
Consolidated Script: Track, Report, and Label Issues Moved from Operations to Support
This script performs three main tasks in sequence:
1. Find all TS issues and extract issue type changes from Operations to Support
2. Generate monthly reports for issues that are currently Support Tickets
3. Add 'moved_from_ops' label to identified issues
"""

import os
import pandas as pd
import requests
import time
import json
import logging
import asyncio
from typing import List, Dict, Optional, Tuple, Generator, Any
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from dateutil import parser as date_parser
from zoneinfo import ZoneInfo

try:
    import aiohttp
except ImportError:
    aiohttp = None

# Jira configuration
JIRA_URL = "https://certifyos.atlassian.net"
JIRA_EMAIL = os.getenv("JIRA_EMAIL", "anura@certifyos.com")
JIRA_API_TOKEN = os.getenv("JIRA_API_TOKEN", "ATATT3xFfGF0")

# Allowed responders - only first responses from these people will be tracked
ALLOWED_RESPONDER_IDS = {
    "712020:c0bede50-504a-4050-9cfb-663b18068ef0",
    "712020:26f1b4bd-7dcb-4fcd-ad32-f9b955cdf191",
    "712020:03726a8e-b3b7-491b-a547-18fcfe483822",
    "64233fa00e6828ab20272dc3",
    "712020:aa21c7ce-2ad5-4062-ac5c-2a94b37764fa"
}

# ============================================================
# TIMESTAMP FORMATTING UTILITIES
# ============================================================

class TimestampFormatter:
    """Convert ISO 8601 timestamps to human-readable format with timezone names"""
    
    # Timezone abbreviations
    TIMEZONE_NAMES = {
        '-0700': 'PDT',  # Pacific Daylight Time
        '-0600': 'CDT',  # Central Daylight Time
        '-0500': 'EDT',  # Eastern Daylight Time
        '+0530': 'IST',  # Indian Standard Time
        '+0000': 'UTC',  # Coordinated Universal Time
        '+00:00': 'UTC',
        '-07:00': 'PDT',
        '-06:00': 'CDT',
        '-05:00': 'EDT',
        '+05:30': 'IST',
    }
    
    MONTH_NAMES = [
        'January', 'February', 'March', 'April', 'May', 'June',
        'July', 'August', 'September', 'October', 'November', 'December'
    ]
    
    @staticmethod
    def format_timestamp(timestamp_str: str, target_timezone: str = 'PDT') -> str:
        """
        Convert ISO 8601 timestamp to human-readable format
        
        Args:
            timestamp_str: ISO 8601 timestamp (e.g., "2025-10-06T15:27:06.885-0700")
            target_timezone: Target timezone ('PDT', 'IST', 'UTC', etc.)
        
        Returns:
            Formatted string like "October 6, 2025 at 3:27 PM PDT"
        """
        try:
            if not timestamp_str or pd.isna(timestamp_str):
                return ''
            
            # Parse the timestamp
            dt = date_parser.isoparse(str(timestamp_str))
            
            # Extract date and time components
            day = dt.day
            month = TimestampFormatter.MONTH_NAMES[dt.month - 1]
            year = dt.year
            hour = dt.hour
            minute = dt.minute
            second = dt.second
            
            # Format time with AM/PM
            am_pm = 'AM' if hour < 12 else 'PM'
            hour_12 = hour if hour <= 12 else hour - 12
            if hour_12 == 0:
                hour_12 = 12
            
            # Get timezone from original string
            tz = target_timezone
            if '-0700' in str(timestamp_str) or '-07:00' in str(timestamp_str):
                tz = 'PDT'
            elif '+0530' in str(timestamp_str) or '+05:30' in str(timestamp_str):
                tz = 'IST'
            elif '+00:00' in str(timestamp_str):
                tz = 'UTC'
            
            # Format the result
            return f"{month} {day}, {year} at {hour_12}:{minute:02d}:{second:02d} {am_pm} {tz}"
        
        except Exception as e:
            logging.debug(f"Error formatting timestamp '{timestamp_str}': {e}")
            return timestamp_str
    
    @staticmethod
    def format_timestamp_with_tz_conversion(timestamp_str: str, from_tz: str = 'PDT', to_tz: str = 'IST') -> str:
        """
        Convert ISO 8601 timestamp and convert timezone
        
        Args:
            timestamp_str: ISO 8601 timestamp
            from_tz: Source timezone
            to_tz: Target timezone
        
        Returns:
            Formatted string with converted timezone
        """
        try:
            if not timestamp_str or pd.isna(timestamp_str):
                return ''
            
            dt = date_parser.isoparse(str(timestamp_str))
            
            # Define timezone offsets (can be expanded)
            tz_offsets = {
                'PDT': timedelta(hours=-7),
                'EDT': timedelta(hours=-4),
                'UTC': timedelta(hours=0),
                'IST': timedelta(hours=5, minutes=30),
            }
            
            # Convert if needed
            if from_tz in tz_offsets and to_tz in tz_offsets:
                offset_diff = tz_offsets[to_tz] - tz_offsets[from_tz]
                dt = dt + offset_diff
                tz = to_tz
            else:
                tz = from_tz
            
            # Extract components
            day = dt.day
            month = TimestampFormatter.MONTH_NAMES[dt.month - 1]
            year = dt.year
            hour = dt.hour
            minute = dt.minute
            second = dt.second
            
            # Format time with AM/PM
            am_pm = 'AM' if hour < 12 else 'PM'
            hour_12 = hour if hour <= 12 else hour - 12
            if hour_12 == 0:
                hour_12 = 12
            
            return f"{month} {day}, {year} at {hour_12}:{minute:02d}:{second:02d} {am_pm} {tz}"
        
        except Exception as e:
            logging.debug(f"Error converting timestamp '{timestamp_str}': {e}")
            return timestamp_str

class ConsolidatedOpsToSupportTracker:
    def __init__(self, project_key: str = "TS", base_date: str = "2025-10-24"):
        self.project_key = project_key
        self.session = requests.Session()
        self.session.auth = (JIRA_EMAIL, JIRA_API_TOKEN)
        self.session.headers.update({
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        })
        
        # Allowed responders for filtering first responses
        self.allowed_responder_ids = ALLOWED_RESPONDER_IDS
        
        # Base configuration for auto-incrementing range
        self.base_date = datetime.strptime(base_date, "%Y-%m-%d")
        self.base_range = 46000  # Starting range as of base_date
        self.weekly_increment = 1000  # Increase by 1000 every week
        
        # Calculate current end range based on weeks elapsed
        self.end_range = self.calculate_end_range()
        
        # File paths (dynamic based on current range)
        self.issues_file = f"{project_key.lower()}_issues_1_to_{self.end_range}.csv"
        self.changes_file = f"{project_key.lower()}_issue_type_changes_1_to_{self.end_range}.csv"
    
    def calculate_end_range(self) -> int:
        """Calculate the end range based on weeks elapsed since base date"""
        current_date = datetime.now()
        weeks_elapsed = (current_date - self.base_date).days // 7
        calculated_range = self.base_range + (weeks_elapsed * self.weekly_increment)
        return calculated_range
    
    # ============================================================
    # KEY ENUMERATION AND BACKFILL METHODS
    # ============================================================
    
    def _fetch_all_issues_by_key_enumeration(self, latest_ticket_num: int, 
                                             fields_param: Optional[List[str]] = None,
                                             include_changelog: bool = True) -> Generator[Dict[str, Any], None, None]:
        """Enumerate issue keys TS-1..TS-latest and fetch existing ones.
        
        This bypasses pagination and date quirks by directly fetching each key.
        Non-existent keys will be skipped efficiently via 404 handling.
        """
        if fields_param is None:
            fields_param = ['summary', 'issuetype', 'status', 'created', 'updated', 
                           'assignee', 'reporter', 'customfield_10024', 'comment']
        
        fetched = 0
        missing = 0
        start = 1
        
        # Try to start near the first known TS ticket to save calls
        try:
            start = max(1, latest_ticket_num - 60000)  # safe guard; most TS projects start low
        except Exception:
            start = 1
        
        # If we know projects typically start at 1, use that
        start = 1
        
        logging.info(f"🔢 Enumerating keys from {self.project_key}-{start} to {self.project_key}-{latest_ticket_num}")
        fields = ','.join(fields_param)
        expands = 'changelog' if include_changelog else None
        
        for num in range(start, latest_ticket_num + 1):
            key = f"{self.project_key}-{num}"
            try:
                # GET /rest/api/3/issue/{issueIdOrKey}
                params = {'fields': fields}
                if expands:
                    params['expand'] = expands
                
                resp = self.session.get(f"{JIRA_URL}/rest/api/3/issue/{key}", 
                                       params=params, timeout=20)
                
                if resp.status_code == 404:
                    missing += 1
                    if num % 1000 == 0:
                        logging.info(f"   ... up to {key}: fetched {fetched}, missing {missing}")
                    continue
                
                resp.raise_for_status()
                issue = resp.json()
                
                # Normalize to standard shape
                if 'key' not in issue:
                    issue['key'] = key
                if include_changelog and 'changelog' not in issue:
                    issue['changelog'] = {'histories': []}
                
                yield issue
                fetched += 1
                if fetched % 200 == 0:
                    logging.info(f"   ... enumerated to {key}: fetched {fetched}, missing {missing}")
                    
            except requests.HTTPError as http_err:
                status = getattr(http_err.response, 'status_code', None)
                if status == 404:
                    missing += 1
                    continue
                logging.warning(f"⚠️ Error fetching {key}: {http_err}")
            except Exception as e:
                logging.warning(f"⚠️ Error on {key}: {e}")
        
        logging.info(f"🔚 Enumeration finished. Fetched: {fetched}, missing: {missing}")
    
    def _backfill_missing_issues(self, issue_numbers: List[int], 
                                fields_param: Optional[List[str]] = None,
                                include_changelog: bool = True) -> List[Dict[str, Any]]:
        """Backfill/retry fetching specific issue numbers that were previously missed.
        
        This method attempts to re-fetch issues that may have been temporarily unavailable.
        """
        if fields_param is None:
            fields_param = ['summary', 'issuetype', 'status', 'created', 'updated', 
                           'assignee', 'reporter', 'customfield_10024', 'comment']
        
        backfilled_issues = []
        fields = ','.join(fields_param)
        expands = 'changelog' if include_changelog else None
        
        logging.info(f"🔄 Attempting to backfill {len(issue_numbers)} missing issues...")
        
        for num in issue_numbers:
            key = f"{self.project_key}-{num}"
            try:
                params = {'fields': fields}
                if expands:
                    params['expand'] = expands
                
                resp = self.session.get(f"{JIRA_URL}/rest/api/3/issue/{key}", 
                                       params=params, timeout=20)
                
                if resp.status_code == 404:
                    logging.debug(f"   ⏭️  {key} still missing (404)")
                    continue
                
                resp.raise_for_status()
                issue = resp.json()
                
                # Normalize
                if 'key' not in issue:
                    issue['key'] = key
                if include_changelog and 'changelog' not in issue:
                    issue['changelog'] = {'histories': []}
                
                backfilled_issues.append(issue)
                logging.info(f"   ✅ Backfilled {key}")
                
            except Exception as e:
                logging.debug(f"   ❌ Could not backfill {key}: {e}")
        
        logging.info(f"✅ Backfill complete. Retrieved {len(backfilled_issues)} additional issues.")
        return backfilled_issues
    
    # ============================================================
    # PARALLEL PROCESSING METHODS
    # ============================================================
    
    def _fetch_issue_parallel_worker(self, issue_num: int, fields_param: List[str], 
                                    include_changelog: bool) -> Optional[Dict[str, Any]]:
        """Worker function to fetch a single issue (used in parallel processing)"""
        key = f"{self.project_key}-{issue_num}"
        try:
            params = {'fields': ','.join(fields_param)}
            if include_changelog:
                params['expand'] = 'changelog'
            
            resp = self.session.get(f"{JIRA_URL}/rest/api/3/issue/{key}", 
                                   params=params, timeout=20)
            
            if resp.status_code == 404:
                return None
            
            resp.raise_for_status()
            issue = resp.json()
            
            if 'key' not in issue:
                issue['key'] = key
            if include_changelog and 'changelog' not in issue:
                issue['changelog'] = {'histories': []}
            
            return issue
        except Exception as e:
            logging.debug(f"⚠️ Error fetching {key}: {e}")
            return None
    
    def _fetch_all_issues_parallel(self, latest_ticket_num: int, 
                                  fields_param: Optional[List[str]] = None,
                                  include_changelog: bool = True,
                                  max_workers: int = 20) -> Generator[Dict[str, Any], None, None]:
        """Fetch all issues using parallel processing with ThreadPoolExecutor
        
        This method uses multiple threads to fetch issues concurrently,
        significantly faster than sequential enumeration.
        """
        if fields_param is None:
            fields_param = ['summary', 'issuetype', 'status', 'created', 'updated', 
                           'assignee', 'reporter', 'customfield_10024', 'comment']
        
        fetched = 0
        missing = 0
        
        logging.info(f"🚀 Starting parallel fetch with {max_workers} workers for {latest_ticket_num} issues...")
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            futures = {
                executor.submit(self._fetch_issue_parallel_worker, i, fields_param, include_changelog): i
                for i in range(1, latest_ticket_num + 1)
            }
            
            # Process results as they complete
            for future in as_completed(futures):
                issue_num = futures[future]
                try:
                    issue = future.result()
                    if issue:
                        yield issue
                        fetched += 1
                        if fetched % 500 == 0:
                            logging.info(f"   ✓ Fetched {fetched} issues, missing {missing}")
                    else:
                        missing += 1
                except Exception as e:
                    logging.debug(f"⚠️ Error processing issue {issue_num}: {e}")
                    missing += 1
        
        logging.info(f"🔚 Parallel fetch complete. Fetched: {fetched}, missing: {missing}")
    
    # ============================================================
    # ASYNC PROCESSING METHODS (Fastest)
    # ============================================================
    
    async def _fetch_issue_async(self, session: 'aiohttp.ClientSession', issue_num: int, 
                                fields_param: List[str], include_changelog: bool) -> Optional[Dict[str, Any]]:
        """Async worker to fetch a single issue"""
        key = f"{self.project_key}-{issue_num}"
        try:
            fields = ','.join(fields_param)
            url = f"{JIRA_URL}/rest/api/3/issue/{key}"
            params = {'fields': fields}
            if include_changelog:
                params['expand'] = 'changelog'
            
            async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                if resp.status == 404:
                    return None
                
                if resp.status == 200:
                    issue = await resp.json()
                    if 'key' not in issue:
                        issue['key'] = key
                    if include_changelog and 'changelog' not in issue:
                        issue['changelog'] = {'histories': []}
                    return issue
                else:
                    logging.debug(f"⚠️ Status {resp.status} for {key}")
                    return None
        except asyncio.TimeoutError:
            logging.debug(f"⏱️ Timeout fetching {key}")
            return None
        except Exception as e:
            logging.debug(f"⚠️ Error fetching {key}: {e}")
            return None
    
    async def _fetch_all_issues_async_batch(self, latest_ticket_num: int, 
                                           fields_param: Optional[List[str]] = None,
                                           include_changelog: bool = True,
                                           batch_size: int = 50) -> Generator[Dict[str, Any], None, None]:
        """Async fetch all issues with batch processing to avoid rate limiting
        
        This method uses asyncio for concurrent HTTP requests, significantly
        faster than parallel threading. Batches requests to avoid overwhelming the server.
        """
        if fields_param is None:
            fields_param = ['summary', 'issuetype', 'status', 'created', 'updated', 
                           'assignee', 'reporter', 'customfield_10024', 'comment']
        
        fetched = 0
        missing = 0
        
        logging.info(f"⚡ Starting async fetch with batch_size={batch_size} for {latest_ticket_num} issues...")
        
        # Create connector and session with connection pooling
        connector = aiohttp.TCPConnector(limit=batch_size, limit_per_host=batch_size)
        auth = aiohttp.BasicAuth(JIRA_EMAIL, JIRA_API_TOKEN)
        
        async with aiohttp.ClientSession(connector=connector, auth=auth) as session:
            # Process in batches
            for batch_start in range(1, latest_ticket_num + 1, batch_size):
                batch_end = min(batch_start + batch_size, latest_ticket_num + 1)
                batch_range = range(batch_start, batch_end)
                
                # Create tasks for this batch
                tasks = [
                    self._fetch_issue_async(session, i, fields_param, include_changelog)
                    for i in batch_range
                ]
                
                # Wait for batch to complete
                results = await asyncio.gather(*tasks, return_exceptions=False)
                
                # Yield successful results
                for issue in results:
                    if issue:
                        yield issue
                        fetched += 1
                    else:
                        missing += 1
                
                # Progress logging
                if fetched % 500 == 0:
                    logging.info(f"   ⚡ Async fetched {fetched} issues, missing {missing}")
                
                # Small delay between batches to avoid rate limiting
                await asyncio.sleep(0.1)
        
        logging.info(f"🔚 Async fetch complete. Fetched: {fetched}, missing: {missing}")
    
    def _fetch_all_issues_async(self, latest_ticket_num: int, 
                               fields_param: Optional[List[str]] = None,
                               include_changelog: bool = True,
                               batch_size: int = 50) -> Generator[Dict[str, Any], None, None]:
        """Wrapper to run async fetch in a sync context"""
        try:
            # Get or create event loop
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        
        # Run the async generator
        async_gen = self._fetch_all_issues_async_batch(latest_ticket_num, fields_param, 
                                                       include_changelog, batch_size)
        
        # Collect results
        async def collect_all():
            results = []
            async for item in async_gen:
                results.append(item)
            return results
        
        results = loop.run_until_complete(collect_all())
        for item in results:
            yield item
    
    # ============================================================
    # STEP 1: FIND ISSUES AND EXTRACT CHANGES
    # ============================================================
    
    def get_issue_details(self, issue_key: str) -> Optional[Dict]:
        """Get issue details including changelog and comments"""
        url = f"{JIRA_URL}/rest/api/3/issue/{issue_key}?expand=changelog,changelog.histories&fields=comment"
        try:
            response = self.session.get(url, timeout=30)
            if response.status_code == 200:
                return response.json()
            return None
        except Exception as e:
            return None
    
    def get_first_response_from_allowed_responder(self, issue_data: Dict) -> Optional[Tuple[str, str]]:
        """
        Find the first comment from an allowed responder and return its timestamp
        
        Args:
            issue_data: Issue data dictionary with fields including comments
        
        Returns:
            Tuple of (timestamp_string, author_display_name) if found, None otherwise
        """
        try:
            # Get the comments
            fields = issue_data.get('fields', {})
            comment_obj = fields.get('comment', {})
            comments = comment_obj.get('comments', [])
            
            if not comments:
                # No comments, so no responder
                return None
            
            # Sort comments by created date (oldest first) to find the actual first response
            sorted_comments = sorted(comments, key=lambda c: c.get('created', ''))
            
            # Look through all comments to find the first one from an allowed responder
            for comment in sorted_comments:
                author = comment.get('author', {})
                author_id = author.get('accountId', '')
                author_name = author.get('displayName', 'Unknown')
                
                # Check if this author is in the allowed list
                if author_id in self.allowed_responder_ids:
                    # Found the first response from an allowed responder!
                    timestamp = comment.get('created', '')
                    return (timestamp, author_name)
            
            # No comments from allowed responders found
            return None
        except Exception as e:
            logging.debug(f"Error finding first responder: {e}")
            return None
    
    def is_first_responder_allowed(self, issue_data: Dict) -> bool:
        """Check if there is a response from an allowed responder (for backward compatibility)"""
        result = self.get_first_response_from_allowed_responder(issue_data)
        return result is not None
    
    def extract_issue_type_changes(self, issue_key: str, histories: List[Dict]) -> List[Dict]:
        """Extract issue type changes from Operations Ticket to Support Ticket"""
        changes = []
        
        for history in histories:
            for item in history.get('items', []):
                if item.get('field') == 'issuetype':
                    from_type = item.get('fromString', '')
                    to_type = item.get('toString', '')
                    
                    # Check if this is a change from Operations Ticket to Support Ticket
                    if from_type == 'Operations Ticket' and to_type == 'Support Ticket':
                        changes.append({
                            'issue_key': issue_key,
                            'change_timestamp': history.get('created'),
                            'change_author': history.get('author', {}).get('displayName', 'Unknown'),
                            'from_type': from_type,
                            'to_type': to_type
                        })
        
        return changes
    
    def process_issue_range(self, start_num: int, end_num: int) -> Tuple[List[Dict], List[Dict]]:
        """Process a range of issue numbers"""
        found_issues = []
        found_changes = []
        
        for i in range(start_num, end_num + 1):
            issue_key = f"{self.project_key}-{i}"
            issue_data = self.get_issue_details(issue_key)
            
            if issue_data:
                fields = issue_data.get('fields', {})
                
                # Extract issue details
                # Get Date of First Response - prioritize comments from allowed responders
                # First, try to find first comment from allowed responder
                first_response_info = self.get_first_response_from_allowed_responder(issue_data)
                
                if first_response_info:
                    # Found first response from allowed responder in comments
                    date_of_first_response = first_response_info[0]  # timestamp
                    first_responder_name = first_response_info[1]  # author name (for logging)
                else:
                    # Fall back to custom field if no comment found
                    date_of_first_response = fields.get('customfield_10024', '') or ''
                
                issue_details = {
                    'key': issue_key,
                    'summary': fields.get('summary', ''),
                    'issue_type': fields.get('issuetype', {}).get('name', ''),
                    'status': fields.get('status', {}).get('name', ''),
                    'created': fields.get('created', ''),
                    'updated': fields.get('updated', ''),
                    'assignee': fields.get('assignee', {}).get('displayName', '') if fields.get('assignee') else '',
                    'reporter': fields.get('reporter', {}).get('displayName', '') if fields.get('reporter') else '',
                    'date_of_first_response': date_of_first_response
                }
                found_issues.append(issue_details)
                
                # Extract changelog
                changelog = issue_data.get('changelog', {})
                histories = changelog.get('histories', [])
                changes = self.extract_issue_type_changes(issue_key, histories)
                found_changes.extend(changes)
        
        return found_issues, found_changes
    
    def find_all_issues_and_changes(self, start_range: int = 1, end_range: Optional[int] = None, 
                                   batch_size: int = 100, max_workers: int = 10, use_enumeration: bool = True,
                                   fetch_mode: str = 'async'):
        """Find all issues and extract issue type changes
        
        Args:
            fetch_mode: 'sync' (sequential), 'parallel' (ThreadPoolExecutor), or 'async' (asyncio - fastest)
        """
        # Use calculated end_range if not provided
        if end_range is None:
            end_range = self.end_range
        
        print("=" * 80)
        print("STEP 1: FINDING ISSUES AND EXTRACTING CHANGES")
        print("=" * 80)
        print(f"📊 Searching for {self.project_key} issues from {start_range} to {end_range}")
        print(f"   (Auto-calculated based on {(datetime.now() - self.base_date).days // 7} weeks since {self.base_date.strftime('%Y-%m-%d')})")
        print(f"   Fetch mode: {fetch_mode.upper()}")
        
        all_issues = []
        all_changes = []
        missed_issue_numbers = []
        
        # Determine fetching strategy
        if use_enumeration:
            fields_to_fetch = ['summary', 'issuetype', 'status', 'created', 'updated', 
                              'assignee', 'reporter', 'customfield_10024', 'comment']
            
            # Select fetch mode
            if fetch_mode.lower() == 'async':
                print(f"\n⚡ Using ASYNC fetching (fastest)...")
                issue_generator = self._fetch_all_issues_async(end_range, fields_to_fetch, True, batch_size=50)
            elif fetch_mode.lower() == 'parallel':
                print(f"\n🚀 Using PARALLEL fetching with {max_workers} workers...")
                issue_generator = self._fetch_all_issues_parallel(end_range, fields_to_fetch, True, max_workers)
            else:  # sync (default fallback)
                print(f"\n🔢 Using SYNC sequential fetching...")
                issue_generator = self._fetch_all_issues_by_key_enumeration(end_range, fields_to_fetch, True)
            
            # Process fetched issues
            for issue_data in issue_generator:
                # Extract issue details
                fields = issue_data.get('fields', {})
                issue_key = issue_data.get('key', '')
                
                # Get Date of First Response - prioritize comments from allowed responders
                # First, try to find first comment from allowed responder
                first_response_info = self.get_first_response_from_allowed_responder(issue_data)
                
                if first_response_info:
                    # Found first response from allowed responder in comments
                    date_of_first_response = first_response_info[0]  # timestamp
                else:
                    # Fall back to custom field if no comment found
                    date_of_first_response = fields.get('customfield_10024', '') or ''
                
                issue_details = {
                    'key': issue_key,
                    'summary': fields.get('summary', ''),
                    'issue_type': fields.get('issuetype', {}).get('name', ''),
                    'status': fields.get('status', {}).get('name', ''),
                    'created': fields.get('created', ''),
                    'updated': fields.get('updated', ''),
                    'assignee': fields.get('assignee', {}).get('displayName', '') if fields.get('assignee') else '',
                    'reporter': fields.get('reporter', {}).get('displayName', '') if fields.get('reporter') else '',
                    'date_of_first_response': date_of_first_response
                }
                all_issues.append(issue_details)
                
                # Extract changelog
                changelog = issue_data.get('changelog', {})
                histories = changelog.get('histories', [])
                changes = self.extract_issue_type_changes(issue_key, histories)
                all_changes.extend(changes)
                
                if len(all_issues) % 500 == 0:
                    print(f"   Progress: {len(all_issues)} issues, {len(all_changes)} changes")
        
        else:
            # Fall back to batch processing
            print(f"\n📦 Using batch processing method...")
            
            # Create batches
            batches = []
            for i in range(start_range, end_range + 1, batch_size):
                batch_end = min(i + batch_size - 1, end_range)
                batches.append((i, batch_end))
            
            print(f"📦 Processing {len(batches)} batches with {max_workers} workers...")
            
            # Process batches in parallel
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_batch = {
                    executor.submit(self.process_issue_range, start, end): (start, end)
                    for start, end in batches
                }
                
                completed = 0
                for future in as_completed(future_to_batch):
                    start, end = future_to_batch[future]
                    try:
                        issues, changes = future.result()
                        all_issues.extend(issues)
                        all_changes.extend(changes)
                        completed += 1
                        
                        if completed % 10 == 0 or completed == len(batches):
                            print(f"   Progress: {completed}/{len(batches)} batches | "
                                  f"Found {len(all_issues)} issues, {len(all_changes)} changes")
                    except Exception as e:
                        print(f"   ❌ Error processing batch {start}-{end}: {e}")
        
        # Attempt backfill for any previously missed issues
        if missed_issue_numbers:
            print(f"\n🔄 Attempting backfill for {len(missed_issue_numbers)} potentially missed issues...")
            backfilled = self._backfill_missing_issues(missed_issue_numbers)
            
            # Process backfilled issues
            for issue_data in backfilled:
                fields = issue_data.get('fields', {})
                issue_key = issue_data.get('key', '')
                
                # Get Date of First Response - prioritize comments from allowed responders
                # First, try to find first comment from allowed responder
                first_response_info = self.get_first_response_from_allowed_responder(issue_data)
                
                if first_response_info:
                    # Found first response from allowed responder in comments
                    date_of_first_response = first_response_info[0]  # timestamp
                else:
                    # Fall back to custom field if no comment found
                    date_of_first_response = fields.get('customfield_10024', '') or ''
                
                issue_details = {
                    'key': issue_key,
                    'summary': fields.get('summary', ''),
                    'issue_type': fields.get('issuetype', {}).get('name', ''),
                    'status': fields.get('status', {}).get('name', ''),
                    'created': fields.get('created', ''),
                    'updated': fields.get('updated', ''),
                    'assignee': fields.get('assignee', {}).get('displayName', '') if fields.get('assignee') else '',
                    'reporter': fields.get('reporter', {}).get('displayName', '') if fields.get('reporter') else '',
                    'date_of_first_response': date_of_first_response
                }
                all_issues.append(issue_details)
                
                changelog = issue_data.get('changelog', {})
                histories = changelog.get('histories', [])
                changes = self.extract_issue_type_changes(issue_key, histories)
                all_changes.extend(changes)
            
            print(f"   ✅ Backfill added {len(backfilled)} issues")
        
        # Save results
        print(f"\n💾 Saving results...")
        
        # Save issues
        df_issues = pd.DataFrame(all_issues)
        df_issues.to_csv(self.issues_file, index=False)
        print(f"   ✅ Saved {len(all_issues)} issues to {self.issues_file}")
        
        # Save changes
        df_changes = pd.DataFrame(all_changes)
        df_changes.to_csv(self.changes_file, index=False)
        print(f"   ✅ Saved {len(all_changes)} changes to {self.changes_file}")
        
        return df_issues, df_changes
    
    # ============================================================
    # STEP 2: GENERATE MONTHLY REPORTS
    # ============================================================
    
    def generate_monthly_reports(self, target_month: Optional[str] = None):
        """Generate monthly reports for issues moved from Ops to Support"""
        print("\n" + "=" * 80)
        print("STEP 2: GENERATING MONTHLY REPORTS")
        print("=" * 80)
        
        # Load data
        print(f"📊 Loading data from CSV files...")
        
        # Check if files exist and are not empty
        if not os.path.exists(self.issues_file):
            print(f"❌ Error: Issues file not found: {self.issues_file}")
            print(f"   Please run Step 1 first to fetch issues.")
            return []
        
        if not os.path.exists(self.changes_file):
            print(f"❌ Error: Changes file not found: {self.changes_file}")
            print(f"   Please run Step 1 first to fetch issues.")
            return []
        
        # Check if files are empty
        if os.path.getsize(self.issues_file) == 0:
            print(f"❌ Error: Issues file is empty: {self.issues_file}")
            print(f"   No issues were fetched in Step 1. Please check your Jira connection and try again.")
            return []
        
        if os.path.getsize(self.changes_file) == 0:
            print(f"❌ Error: Changes file is empty: {self.changes_file}")
            print(f"   No changes were found in Step 1.")
            return []
        
        try:
            df_issues = pd.read_csv(self.issues_file)
            df_changes = pd.read_csv(self.changes_file)
        except pd.errors.EmptyDataError:
            print(f"❌ Error: CSV files are empty or have no columns.")
            print(f"   No issues were fetched in Step 1. Please check your Jira connection and try again.")
            return []
        
        print(f"   ✅ Loaded {len(df_issues)} issues")
        print(f"   ✅ Loaded {len(df_changes)} changes")
        
        # Check if we have any data
        if len(df_changes) == 0:
            print(f"⚠️  Warning: No issue type changes found.")
            print(f"   No reports will be generated.")
            return []
        
        # Convert timestamp to datetime
        df_changes['change_timestamp'] = pd.to_datetime(df_changes['change_timestamp'], utc=True)
        df_changes['year_month'] = df_changes['change_timestamp'].dt.to_period('M')
        
        # Get available months
        available_months = df_changes['year_month'].value_counts().sort_index()
        
        print(f"\n📅 Available months:")
        for month, count in available_months.items():
            print(f"   - {month}: {count} changes")
        
        # Determine which months to process
        if target_month:
            months_to_process = [pd.Period(target_month, freq='M')]
            print(f"\n🎯 Processing target month: {target_month}")
        else:
            # Process last 3 months
            months_to_process = available_months.index[-3:].tolist()
            print(f"\n🎯 Processing last 3 months: {', '.join(str(m) for m in months_to_process)}")
        
        generated_reports = []
        
        for year_month in months_to_process:
            report_file = self.generate_single_month_report(df_issues, df_changes, year_month)
            if report_file:
                generated_reports.append(report_file)
        
        return generated_reports
    
    def generate_single_month_report(self, df_issues: pd.DataFrame, 
                                    df_changes: pd.DataFrame, 
                                    year_month: pd.Period) -> Optional[str]:
        """Generate report for a single month"""
        year_month_str = str(year_month)
        print(f"\n{'=' * 60}")
        print(f"GENERATING REPORT FOR {year_month_str}")
        print(f"{'=' * 60}")
        
        # Filter changes for this month
        monthly_changes = df_changes[df_changes['year_month'] == year_month].copy()
        print(f"✅ Found {len(monthly_changes)} issue type changes for {year_month_str}")
        
        # Create detailed report with filtering
        report_data = []
        
        for _, change in monthly_changes.iterrows():
            issue_key = change['issue_key']
            issue_details = df_issues[df_issues['key'] == issue_key]
            
            if len(issue_details) > 0:
                issue = issue_details.iloc[0]
                
                # Only include if current issue type is "Support Ticket" 
                # and it was changed from "Operations Ticket" to "Support Ticket"
                if (issue['issue_type'] == 'Support Ticket' and 
                    change['from_type'] == 'Operations Ticket' and 
                    change['to_type'] == 'Support Ticket'):
                    
                    report_data.append({
                        'issue_key': issue_key,
                        'summary': issue['summary'],
                        'created': issue['created'],
                        'updated': issue['updated'],
                        'status': issue['status'],
                        'assignee': issue['assignee'],
                        'reporter': issue['reporter'],
                        'current_issue_type': issue['issue_type'],
                        'change_timestamp': change['change_timestamp'],
                        'change_author': change['change_author'],
                        'from_type': change['from_type'],
                        'to_type': change['to_type'],
                        'date_of_first_response': issue.get('date_of_first_response', '')
                    })
        
        if not report_data:
            print(f"⚠️  No qualifying issues found for {year_month_str}")
            return None
        
        # Create DataFrame for the report
        report_df = pd.DataFrame(report_data)
        report_df = report_df.sort_values('change_timestamp')
        
        # Format timestamps for human readability
        report_df = self._format_report_timestamps(report_df)
        
        # Save report
        report_file = f"{self.project_key.lower()}_issue_type_changes_{year_month_str.replace('-', '_')}.csv"
        report_df.to_csv(report_file, index=False)
        print(f"✅ Report saved to {report_file}")
        
        # Generate summary
        summary = self.generate_summary(report_df, year_month_str)
        
        # Save summary
        summary_file = f"{self.project_key.lower()}_summary_{year_month_str.replace('-', '_')}.json"
        with open(summary_file, 'w') as f:
            json.dump(summary, f, indent=2, default=str)
        print(f"✅ Summary saved to {summary_file}")
        
        # Print summary
        self.print_summary(summary, year_month_str)
        
        return report_file
    
    def _format_report_timestamps(self, report_df: pd.DataFrame) -> pd.DataFrame:
        """Format all timestamp columns to human-readable format with timezone"""
        report_df = report_df.copy()
        
        # Timestamp columns to format
        timestamp_columns = {
            'created': 'Created',
            'updated': 'Updated',
            'change_timestamp': 'Changed',
            'date_of_first_response': 'First Response'
        }
        
        # Add formatted columns alongside original timestamps
        for col, label in timestamp_columns.items():
            if col in report_df.columns:
                # Create new formatted column
                formatted_col = f"{col}_formatted"
                report_df[formatted_col] = report_df[col].apply(
                    lambda x: TimestampFormatter.format_timestamp(x) if x else ''
                )
                
                # Also create a column with IST timezone for Indian users
                ist_col = f"{col}_IST"
                report_df[ist_col] = report_df[col].apply(
                    lambda x: TimestampFormatter.format_timestamp_with_tz_conversion(x, 'PDT', 'IST') if x else ''
                )
        
        # Reorder columns: put formatted versions right after originals
        cols = list(report_df.columns)
        new_order = []
        
        for col in cols:
            if col not in [c for c in cols if c.endswith('_formatted') or c.endswith('_IST')]:
                new_order.append(col)
                # Add formatted version if exists
                if f"{col}_formatted" in cols:
                    new_order.append(f"{col}_formatted")
                # Add IST version if exists
                if f"{col}_IST" in cols:
                    new_order.append(f"{col}_IST")
        
        return report_df[new_order]
    
    def generate_summary(self, report_df: pd.DataFrame, year_month_str: str) -> Dict:
        """Generate summary statistics"""
        summary = {
            'month': year_month_str,
            'total_changes': len(report_df),
            'changes_by_author': report_df['change_author'].value_counts().to_dict(),
            'changes_by_status': report_df['status'].value_counts().to_dict(),
            'all_changes': []
        }
        
        for _, row in report_df.iterrows():
            summary['all_changes'].append({
                'issue_key': row['issue_key'],
                'summary': row['summary'],
                'change_timestamp': str(row['change_timestamp']),
                'change_author': row['change_author'],
                'status': row['status']
            })
        
        return summary
    
    def print_summary(self, summary: Dict, year_month_str: str):
        """Print summary to console"""
        print(f"\nSUMMARY FOR {year_month_str}:")
        print(f"  Total changes: {summary['total_changes']}")
        
        print(f"  Changes by author:")
        for author, count in sorted(summary['changes_by_author'].items(), 
                                    key=lambda x: x[1], reverse=True):
            print(f"    - {author}: {count}")
        
        print(f"  Changes by current status:")
        for status, count in sorted(summary['changes_by_status'].items(), 
                                    key=lambda x: x[1], reverse=True):
            print(f"    - {status}: {count}")
    
    # ============================================================
    # STEP 3: ADD LABELS TO ISSUES
    # ============================================================
    
    def get_issue_labels(self, issue_key: str) -> List[str]:
        """Get current labels for an issue"""
        url = f"{JIRA_URL}/rest/api/3/issue/{issue_key}?fields=labels"
        try:
            response = self.session.get(url, timeout=30)
            if response.status_code == 200:
                data = response.json()
                return data['fields'].get('labels', [])
            return []
        except Exception:
            return []
    
    def add_label_to_issue(self, issue_key: str, new_label: str) -> bool:
        """Add a label to an issue without removing existing labels"""
        current_labels = self.get_issue_labels(issue_key)
        
        if not current_labels:
            # If no labels exist, create with just the new label
            current_labels = []
        
        # Check if label already exists
        if new_label in current_labels:
            return True
        
        # Add the new label to existing labels
        updated_labels = current_labels + [new_label]
        
        # Update the issue
        url = f"{JIRA_URL}/rest/api/3/issue/{issue_key}"
        payload = {"fields": {"labels": updated_labels}}
        
        try:
            response = self.session.put(url, json=payload, timeout=30)
            return response.status_code == 204
        except Exception:
            return False
    
    def add_labels_from_reports(self, report_files: List[str], label: str = "moved_from_ops"):
        """Add labels to all issues in the generated reports"""
        print("\n" + "=" * 80)
        print("STEP 3: ADDING LABELS TO ISSUES")
        print("=" * 80)
        
        all_issue_keys = set()
        
        # Collect all unique issue keys from all reports
        for report_file in report_files:
            if os.path.exists(report_file):
                df = pd.read_csv(report_file)
                issue_keys = df['issue_key'].tolist()
                all_issue_keys.update(issue_keys)
                print(f"📋 Loaded {len(issue_keys)} issues from {report_file}")
        
        issue_keys_list = sorted(list(all_issue_keys))
        print(f"\n🏷️  Processing {len(issue_keys_list)} unique issues to add label '{label}'")
        
        success_count = 0
        failed_count = 0
        already_exists_count = 0
        
        for i, issue_key in enumerate(issue_keys_list, 1):
            # Check if label already exists
            current_labels = self.get_issue_labels(issue_key)
            
            if label in current_labels:
                already_exists_count += 1
                if i % 10 == 0 or i == len(issue_keys_list):
                    print(f"   Progress: {i}/{len(issue_keys_list)} | "
                          f"Added: {success_count}, Exists: {already_exists_count}, Failed: {failed_count}")
            else:
                if self.add_label_to_issue(issue_key, label):
                    success_count += 1
                    if i % 10 == 0 or i == len(issue_keys_list):
                        print(f"   Progress: {i}/{len(issue_keys_list)} | "
                              f"Added: {success_count}, Exists: {already_exists_count}, Failed: {failed_count}")
                else:
                    failed_count += 1
            
            # Small delay to avoid rate limiting
            time.sleep(0.3)
        
        print(f"\n📈 LABEL ADDITION SUMMARY:")
        print(f"   ✅ Successfully added: {success_count}")
        print(f"   ℹ️  Already existed: {already_exists_count}")
        print(f"   ❌ Failed: {failed_count}")
        print(f"   📊 Total processed: {len(issue_keys_list)}")
    
    # ============================================================
    # MAIN EXECUTION
    # ============================================================
    
    def run_full_process(self, skip_fetch: bool = False, target_month: Optional[str] = None, skip_label: bool = False,
                         fetch_mode: str = 'async', max_workers: int = 20):
        """Run the complete process: find, report, and label"""
        print("\n" + "=" * 80)
        print("🚀 CONSOLIDATED OPS TO SUPPORT TRACKER")
        print("=" * 80)
        print(f"Project: {self.project_key}")
        print(f"Target: Issues moved from Operations Ticket to Support Ticket")
        print(f"Label to add: moved_from_ops")
        print(f"Auto-calculated range: 1 to {self.end_range}")
        print(f"  (Base: {self.base_range} on {self.base_date.strftime('%Y-%m-%d')}, +{self.weekly_increment}/week)")
        print("=" * 80)
        
        start_time = time.time()
        
        # Step 1: Find issues and changes (can be skipped if files exist)
        if skip_fetch and os.path.exists(self.issues_file) and os.path.exists(self.changes_file):
            print(f"\n⏭️  Skipping Step 1 - Using existing files:")
            print(f"   - {self.issues_file}")
            print(f"   - {self.changes_file}")
        else:
            self.find_all_issues_and_changes(fetch_mode=fetch_mode, max_workers=max_workers)
        
        # Step 2: Generate monthly reports
        report_files = self.generate_monthly_reports(target_month)
        
        # Step 3: Add labels to issues
        if skip_label:
            print("\n⏭️  Skipping Step 3 - Label addition disabled (--skip-label)")
        elif report_files:
            self.add_labels_from_reports(report_files)
        else:
            print("\n⚠️  No reports generated, skipping label addition")
        
        # Final summary
        elapsed_time = time.time() - start_time
        print("\n" + "=" * 80)
        print("🎉 PROCESS COMPLETED SUCCESSFULLY!")
        print("=" * 80)
        print(f"⏱️  Total execution time: {elapsed_time:.2f} seconds")
        print(f"📁 Generated files:")
        print(f"   - {self.issues_file}")
        print(f"   - {self.changes_file}")
        for report_file in report_files:
            print(f"   - {report_file}")
        print("=" * 80)


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Consolidated script to track, report, and label issues moved from Ops to Support'
    )
    parser.add_argument(
        '--skip-fetch',
        action='store_true',
        help='Skip fetching issues from Jira (use existing CSV files)'
    )
    parser.add_argument(
        '--skip-label',
        action='store_true',
        help='Skip adding labels to Jira issues (only generate reports)'
    )
    parser.add_argument(
        '--month',
        type=str,
        help='Target specific month for report (format: YYYY-MM, e.g., 2025-10)'
    )
    parser.add_argument(
        '--project',
        type=str,
        default='TS',
        help='Project key (default: TS)'
    )
    parser.add_argument(
        '--base-date',
        type=str,
        default='2025-10-24',
        help='Base date for range calculation (format: YYYY-MM-DD, default: 2025-10-24)'
    )
    parser.add_argument(
        '--base-range',
        type=int,
        help='Override base range (default: 46000)'
    )
    parser.add_argument(
        '--weekly-increment',
        type=int,
        help='Override weekly increment (default: 1000)'
    )
    parser.add_argument(
        '--fetch-mode',
        type=str,
        choices=['sync', 'parallel', 'async'],
        default='async',
        help='Fetching strategy: sync (sequential), parallel (threading), async (asyncio - fastest). Default: async'
    )
    parser.add_argument(
        '--max-workers',
        type=int,
        default=20,
        help='Max workers for parallel mode (default: 20)'
    )
    
    args = parser.parse_args()
    
    # Create tracker instance
    tracker = ConsolidatedOpsToSupportTracker(project_key=args.project, base_date=args.base_date)
    
    # Override base_range and weekly_increment if provided
    if args.base_range:
        tracker.base_range = args.base_range
        tracker.end_range = tracker.calculate_end_range()
        tracker.issues_file = f"{args.project.lower()}_issues_1_to_{tracker.end_range}.csv"
        tracker.changes_file = f"{args.project.lower()}_issue_type_changes_1_to_{tracker.end_range}.csv"
    
    if args.weekly_increment:
        tracker.weekly_increment = args.weekly_increment
        tracker.end_range = tracker.calculate_end_range()
        tracker.issues_file = f"{args.project.lower()}_issues_1_to_{tracker.end_range}.csv"
        tracker.changes_file = f"{args.project.lower()}_issue_type_changes_1_to_{tracker.end_range}.csv"
    
    # Run the full process with fetch mode
    tracker.run_full_process(skip_fetch=args.skip_fetch, target_month=args.month, 
                            skip_label=args.skip_label, fetch_mode=args.fetch_mode, 
                            max_workers=args.max_workers)


if __name__ == "__main__":
    main()

