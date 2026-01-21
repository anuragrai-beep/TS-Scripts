#!/usr/bin/env python3
"""
Automation to add 'potential_TS_issue' label to Jira Operations Tickets
Uses fuzzy matching with keywords and tracks processed tickets in CSV to avoid rescanning.
Excludes tickets with 'Done' status from labeling.
Excludes tickets with 'TS_SC' or 'script_cleared' labels to prevent re-processing reviewed tickets.
Uses date-based scanning starting from October 1, 2025.
"""

import os
import requests
import json
import time
import glob
import csv
from typing import List, Dict, Optional, Set
from datetime import datetime, timedelta
from fuzzywuzzy import fuzz, process


# Jira configuration
JIRA_URL = "https://certifyos.atlassian.net"
JIRA_EMAIL = os.getenv("JIRA_EMAIL", "anura@certifyos.com")
JIRA_API_TOKEN = os.getenv("JIRA_API_TOKEN", "ATATT3xFfGF0")

# CSV file to track processed tickets
PROCESSED_TICKETS_CSV = "processed_tickets.csv"
PROGRESS_TRACKER_CSV = "scan_progress.csv"

# Default start date - November 21, 2025
DEFAULT_START_DATE = "2025-11-21"

# Common keywords/phrases to detect
KEYWORDS = [
    "cannot submit",
    "can't submit",
    "unable to submit",
    "Finish button",
    "Finish and Submit",
    "stuck on finish",
    "spinning circle",
    "not moving forward",
    "signature screen",
    "attestation",
    "ready to sign",
    "loop of finish edit",
    "missing attestation",
    "not loading",
    "page not loading",
    "form error",
    "form submission error",
    "glitch",
    "expired license",
    "missing PLI",
    "update provider details",
    "update email",
    "wrong spelling",
    "correct name",
    "backfill",
    "provider not credentialed",
    "upload forms",
    "SFTP",
    "API",
    "integration",
    "firewall",
    "endpoint",
    "generate credentials",
    "workflow timelines",
    "automation failing",
    "backend",
    "null value",
    "missing field",
    "business rule",
    "delete facility/group",
    "bulk update",
    "bug",
    "platform issue",
    "can't login",
    "link expired",
    "resend link",
    "verification code not working",
    "workflow audits",
    "API not working",
    "How to submit",
    "Review error",
    "why blocked",
    "how to upload forms",
    "Wrong name",
    "update emai",
    "expired license incorrect",
    "missing data",
    "GET DATA error",
    "PSV Generation errored",
    "fields missing",
    "browser issues",
    "Delete group",
    "update group",
    "bulk changes",
    "Issue with Portal"
]


POTENTIAL_TS_LABEL = "potential_TS_issue"
FUZZY_MATCH_THRESHOLD = 80  # Minimum fuzzy match score (0-100)
BATCH_SIZE = 1000  # Number of tickets to process per run

# Ignore patterns - tickets containing BOTH strings in a pair will be ignored
IGNORE_PATTERNS = [
    ("outreach@certifyos.com", "Action Required: "),
    ("Notice of Canceled", "outreach@certifyos.com"),
    ("Notice of Canceled", "credentialing@certifyos.com"),
    ("credentialing@hioscar.com", "Virtru Secure Email"),
    ("rosters@hioscar.com", "Virtru Secure Email")
]


class OpsTicketLabelAutomation:
    """Automation to add potential_TS_issue label to Operations Tickets based on fuzzy keyword matching"""
    
    def __init__(self):
        """Initialize Jira session and load processed tickets"""
        self.session = requests.Session()
        self.session.auth = (JIRA_EMAIL, JIRA_API_TOKEN)
        self.session.headers.update({
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        })
        
        # Load already processed tickets from CSV
        self.processed_tickets: Set[str] = self.load_processed_tickets()
        
        # Load scan progress
        self.scan_progress = self.load_scan_progress()
        
        # Normalize keywords for case-insensitive matching
        self.keywords_lower = [kw.lower() for kw in KEYWORDS]
    
    def should_ignore_ticket(self, description: str, reporter_email: str = None) -> bool:
        """Check if ticket should be ignored based on IGNORE_PATTERNS or reporter"""
        # Check reporter
        if reporter_email and reporter_email.lower() == 'support@sendgrid.com':
            return True

        if not description:
            return False
        
        desc_lower = description.lower()
        
        # Check each ignore pattern pair
        for pattern1, pattern2 in IGNORE_PATTERNS:
            if pattern1.lower() in desc_lower and pattern2.lower() in desc_lower:
                return True
        
        return False
    
    def load_processed_tickets(self) -> Set[str]:
        """Load already processed ticket keys from CSV file"""
        processed_tickets = set()
        
        if os.path.exists(PROCESSED_TICKETS_CSV):
            try:
                with open(PROCESSED_TICKETS_CSV, 'r', newline='', encoding='utf-8') as csvfile:
                    reader = csv.reader(csvfile)
                    for row in reader:
                        if row:  # Skip empty rows
                            processed_tickets.add(row[0].strip())
                print(f"   📋 Loaded {len(processed_tickets)} previously processed tickets from {PROCESSED_TICKETS_CSV}")
            except Exception as e:
                print(f"   ⚠️  Error loading processed tickets CSV: {e}")
                # Create a new CSV file if it's corrupted
                open(PROCESSED_TICKETS_CSV, 'w').close()
        else:
            print(f"   📋 No existing processed tickets CSV found. Creating new one: {PROCESSED_TICKETS_CSV}")
            # Create CSV file with header
            with open(PROCESSED_TICKETS_CSV, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(['ticket_key', 'processed_date', 'matched_keyword', 'label_added'])
        
        return processed_tickets
    
    def load_scan_progress(self) -> Dict:
        """Load scan progress from tracker file"""
        progress = {
            'last_processed_date': DEFAULT_START_DATE,
            'last_processed_ticket': None,
            'last_scan_date': None,
            'total_processed': 0,
            'current_batch': 0
        }
        
        if os.path.exists(PROGRESS_TRACKER_CSV):
            try:
                with open(PROGRESS_TRACKER_CSV, 'r', newline='', encoding='utf-8') as csvfile:
                    reader = csv.DictReader(csvfile)
                    for row in reader:
                        progress = {
                            'last_processed_date': row.get('last_processed_date', DEFAULT_START_DATE),
                            'last_processed_ticket': row.get('last_processed_ticket'),
                            'last_scan_date': row.get('last_scan_date'),
                            'total_processed': int(row.get('total_processed', 0)),
                            'current_batch': int(row.get('current_batch', 0)) + 1
                        }
                        break
                print(f"   📊 Loaded scan progress: {progress['total_processed']} tickets processed, last date: {progress['last_processed_date']}")
            except Exception as e:
                print(f"   ⚠️  Error loading progress tracker: {e}")
                # Create a new progress tracker
                self.save_scan_progress(progress)
        else:
            print(f"   📊 No existing progress tracker found. Creating new one: {PROGRESS_TRACKER_CSV}")
            print(f"   📅 Default start date: {DEFAULT_START_DATE}")
            self.save_scan_progress(progress)
        
        return progress
    
    def save_scan_progress(self, progress: Dict):
        """Save scan progress to tracker file"""
        try:
            with open(PROGRESS_TRACKER_CSV, 'w', newline='', encoding='utf-8') as csvfile:
                fieldnames = ['last_processed_date', 'last_processed_ticket', 'last_scan_date', 'total_processed', 'current_batch']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerow(progress)
        except Exception as e:
            print(f"   ❌ Error saving scan progress: {e}")
    
    def save_processed_ticket(self, ticket_key: str, matched_keyword: str = "", label_added: bool = False):
        """Save processed ticket to CSV file"""
        try:
            with open(PROCESSED_TICKETS_CSV, 'a', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow([
                    ticket_key, 
                    datetime.now().isoformat(),
                    matched_keyword,
                    str(label_added)  
                ])
            self.processed_tickets.add(ticket_key)
            
            # Update progress
            self.scan_progress['last_processed_ticket'] = ticket_key
            self.scan_progress['last_scan_date'] = datetime.now().isoformat()
            self.scan_progress['total_processed'] += 1
            self.save_scan_progress(self.scan_progress)
        except Exception as e:
            print(f"   ❌ Error saving processed ticket to CSV: {e}")
    
    def get_current_date_range(self) -> tuple:
        """Get the current date range for scanning"""
        # Start from the last processed date or default start date
        start_date = self.scan_progress['last_processed_date']
        
        # End date is today
        end_date = datetime.now().strftime("%Y-%m-%d")
        
        return start_date, end_date
    
    def format_jira_date(self, date_str: str) -> str:
        """Format date for JQL query"""
        try:
            # Parse the date and format for JQL
            date_obj = datetime.strptime(date_str, "%Y-%m-%d")
            return date_obj.strftime("%Y-%m-%d")
        except ValueError:
            # If already in correct format, return as is
            return date_str
    
    def _extract_text_from_jira_content(self, content: Dict) -> str:
        """Extract plain text from Jira's structured content format"""
        if not isinstance(content, dict):
            return str(content) if content else ''
        
        text_parts = []
        content_list = content.get('content', [])
        
        if isinstance(content_list, list):
            for item in content_list:
                if isinstance(item, dict):
                    # Check for text directly
                    text = item.get('text', '')
                    if text:
                        text_parts.append(text)
                    # Check for nested content
                    nested = item.get('content', [])
                    if isinstance(nested, list):
                        for nested_item in nested:
                            if isinstance(nested_item, dict):
                                nested_text = nested_item.get('text', '')
                                if nested_text:
                                    text_parts.append(nested_text)
        
        return ' '.join(text_parts)
    
    def extract_ticket_text(self, issue: Dict) -> str:
        """Extract and combine text from description, summary, and Title fields"""
        fields = issue.get('fields', {})
        
        # Extract summary (standard field)
        summary = fields.get('summary', '') or ''
        
        # Extract description (handle structured content)
        description = fields.get('description', '')
        if isinstance(description, dict):
            desc_text = self._extract_text_from_jira_content(description)
        else:
            desc_text = str(description) if description else ''
        
        # Extract Title (customfield_12046)
        title = fields.get('customfield_12046', '') or ''
        if isinstance(title, dict):
            title_text = self._extract_text_from_jira_content(title)
        else:
            title_text = str(title) if title else ''
        
        # Combine all text
        combined_text = f"{summary} {desc_text} {title_text}"
        
        return combined_text
    
    def fuzzy_check_keywords_match(self, text: str) -> Optional[str]:
        """Check if any keyword fuzzy matches in the text. Returns matched keyword or None"""
        text_lower = text.lower()
        
        # First check for exact matches (faster)
        for keyword in self.keywords_lower:
            if keyword in text_lower:
                return keyword
        
        # If no exact match, try fuzzy matching
        for keyword in self.keywords_lower:
            # Use partial ratio for substring matching
            score = fuzz.partial_ratio(keyword, text_lower)
            if score >= FUZZY_MATCH_THRESHOLD:
                print(f"      🔍 Fuzzy match: '{keyword}' (score: {score})")
                return keyword
        
        return None
    
    def get_issue_labels(self, issue_key: str) -> List[str]:
        """Get current labels for an issue"""
        url = f"{JIRA_URL}/rest/api/3/issue/{issue_key}?fields=labels"
        try:
            response = self.session.get(url, timeout=30)
            if response.status_code == 200:
                data = response.json()
                return data.get('fields', {}).get('labels', [])
            return []
        except Exception as e:
            print(f"   ⚠️  Error getting labels for {issue_key}: {e}")
            return []
    
    def add_potential_ts_label(self, issue_key: str) -> bool:
        """Add potential_TS_issue label to an issue without removing existing labels"""
        # Get current labels
        current_labels = self.get_issue_labels(issue_key)
        
        # Check if label already exists
        if POTENTIAL_TS_LABEL in current_labels:
            return True  # Already labeled
        
        # Add the new label to existing labels
        updated_labels = current_labels + [POTENTIAL_TS_LABEL]
        
        # Update the issue
        url = f"{JIRA_URL}/rest/api/3/issue/{issue_key}"
        payload = {
            "fields": {
                "labels": updated_labels
            }
        }
        
        try:
            response = self.session.put(url, json=payload, timeout=30)
            if response.status_code == 204:
                return True
            else:
                print(f"   ❌ Failed to add potential_TS_issue label to {issue_key}: {response.status_code} - {response.text[:200]}")
                return False
        except Exception as e:
            print(f"   ❌ Error adding potential_TS_issue label to {issue_key}: {e}")
            return False
    
    def find_tickets_to_process(self, batch_size: int = BATCH_SIZE) -> List[Dict]:
        """Find tickets that need processing using date-based scanning"""
        print("=" * 80)
        print("🔍 DATE-BASED TICKET SCANNING")
        print("=" * 80)
        
        # Get current date range
        start_date, end_date = self.get_current_date_range()
        formatted_start = self.format_jira_date(start_date)
        formatted_end = self.format_jira_date(end_date)
        
        print(f"   📅 Scanning date range: {formatted_start} to {formatted_end}")
        print(f"   📦 Batch size: {batch_size} tickets")
        
        # Build JQL query - exclude Done status tickets and use date range
        jql = (
            'project = TS AND '
            'issuetype = "Operations Ticket" AND '
            '(labels IN ("Credentialing_Inbox", "outreach_inbox") OR '
            '"Request Type" IN ("Outreach Inbox Emailed request (TS)", "Credentialing Inbox Emailed request (TS)")) AND '
            'status != Done AND '
            'labels != "script_cleared" AND '
            'labels != "TS_SC" AND '
            f'created >= "{formatted_start}" AND created <= "{formatted_end}" '
            'ORDER BY created ASC, key ASC'  # Process oldest first
        )
        
        url = f"{JIRA_URL}/rest/api/3/search/jql"
        start_at = 0
        max_results = 100
        
        print(f"\n   🎯 JQL Query: {jql}\n")
        
        total_found = None
        tickets_to_process = []
        processed_count = 0
        latest_ticket_date = start_date
        
        while True:
            current_max = max_results
            remaining_batch = batch_size - processed_count
            if remaining_batch < max_results:
                current_max = remaining_batch
            
            params = {
                'jql': jql,
                'startAt': start_at,
                'maxResults': current_max,
                'fields': 'summary,description,customfield_12046,labels,key,status,assignee,reporter,created'
            }
            
            try:
                response = self.session.get(url, params=params, timeout=60)
                if response.status_code != 200:
                    print(f"\n   ❌ Error searching tickets: {response.status_code} - {response.text[:200]}")
                    break
                
                data = response.json()
                issues = data.get('issues', [])
                
                if total_found is None:
                    total_found = data.get('total', len(issues))
                
                if not issues:
                    break
                
                if total_found is not None and start_at == 0:
                    print(f"   📊 Found {total_found} tickets in date range {formatted_start} to {formatted_end}")
                
                # Process each issue
                for issue in issues:
                    issue_key = issue.get('key')
                    if not issue_key:
                        continue
                    
                    # Skip if already processed (from previous runs)
                    if issue_key in self.processed_tickets:
                        processed_count += 1
                        continue
                    
                    fields = issue.get('fields', {})
                    created_date = fields.get('created', '')
                    
                    # Extract date from created timestamp (format: "2024-01-01T10:00:00.000+0000")
                    if created_date:
                        try:
                            ticket_date = created_date.split('T')[0]  # Extract YYYY-MM-DD
                            if ticket_date > latest_ticket_date:
                                latest_ticket_date = ticket_date
                        except:
                            pass
                    
                    # Extract text from fields
                    summary = fields.get('summary', '') or ''
                    
                    # Extract description
                    description = fields.get('description', '')
                    if isinstance(description, dict):
                        desc_text = self._extract_text_from_jira_content(description)
                    else:
                        desc_text = str(description) if description else ''
                    
                    # Extract Title (customfield_12046)
                    title = fields.get('customfield_12046', '') or ''
                    if isinstance(title, dict):
                        title_text = self._extract_text_from_jira_content(title)
                    else:
                        title_text = str(title) if title else ''
                    
                    # Get current labels
                    current_labels = fields.get('labels', [])
                    has_potential_ts_label = POTENTIAL_TS_LABEL in current_labels
                    
                    # Store ticket data
                    ticket_data = {
                        'issue_key': issue_key,
                        'summary': summary,
                        'description': desc_text,
                        'title': title_text,
                        'combined_text': f"{summary} {desc_text} {title_text}",
                        'status': fields.get('status', {}).get('name', ''),
                        'assignee': fields.get('assignee'),
                        'reporter_email': fields.get('reporter', {}).get('emailAddress', '') if fields.get('reporter') else '',
                        'has_potential_ts_label': has_potential_ts_label,
                        'labels': current_labels,
                        'created': created_date
                    }
                    
                    tickets_to_process.append(ticket_data)
                    processed_count += 1
                    
                    if processed_count >= batch_size:
                        break
                
                # Update start_at for next page
                start_at += len(issues)
                
                # Progress update
                progress = f"   ✅ Checked {start_at} tickets | New to process: {len(tickets_to_process)}"
                if total_found:
                    progress += f" | Total in range: {total_found}"
                print(progress, end='\r')
                
                if len(issues) < current_max or processed_count >= batch_size:
                    break
                
                time.sleep(0.3)
                
            except Exception as e:
                print(f"\n   ❌ Error fetching tickets at page {start_at}: {e}")
                start_at += max_results
                time.sleep(1)
                continue
        
        # Update progress with the latest date processed
        if tickets_to_process:
            self.scan_progress['last_processed_date'] = latest_ticket_date
            self.save_scan_progress(self.scan_progress)
        
        print(f"\n\n📊 Batch scan complete:")
        print(f"   • Date range: {formatted_start} to {formatted_end}")
        print(f"   • New tickets to process: {len(tickets_to_process)}")
        print(f"   • Progress updated to date: {latest_ticket_date}")
        if total_found:
            print(f"   • Total tickets in this date range: {total_found}")
        
        return tickets_to_process
    
    def process_tickets(self, dry_run: bool = False, batch_size: int = BATCH_SIZE) -> Dict:
        """Main processing function"""
        print("\n" + "=" * 80)
        print("🏷️  DATE-BASED TS ISSUE LABEL AUTOMATION")
        print("=" * 80)
        print(f"   • Fuzzy match threshold: {FUZZY_MATCH_THRESHOLD}%")
        print(f"   • Batch size: {batch_size} tickets per run")
        print(f"   • Default start date: {DEFAULT_START_DATE}")
        print(f"   • Tracking processed tickets in: {PROCESSED_TICKETS_CSV}")
        print(f"   • Progress tracking in: {PROGRESS_TRACKER_CSV}")
        print(f"   • Excluding tickets with 'Done' status")
        print(f"   • Excluding tickets with 'TS_SC' or 'script_cleared' labels")
        print(f"   • Current progress: {self.scan_progress['total_processed']} tickets processed")
        print(f"   • Current batch: #{self.scan_progress['current_batch']}")
        print(f"   • Last processed date: {self.scan_progress['last_processed_date']}")
        
        if dry_run:
            print("⚠️  DRY RUN MODE - No labels will be added\n")
        
        # Find tickets that need processing using date-based scanning
        tickets_to_process = self.find_tickets_to_process(batch_size=batch_size)
        
        if not tickets_to_process:
            print("✅ No new tickets to process in this batch")
            # Check if we've reached today's date
            start_date, end_date = self.get_current_date_range()
            if start_date >= end_date:
                print("🎉 All tickets up to today have been processed!")
            return {
                'processed': 0,
                'labeled': 0,
                'skipped_already_labeled': 0,
                'skipped_no_match': 0,
                'failed': 0
            }
        
        # Process each ticket
        labeled_count = 0
        skipped_already_labeled = 0
        skipped_no_match = 0
        failed_count = 0
        
        print(f"\n🔍 Processing {len(tickets_to_process)} new tickets in batch #{self.scan_progress['current_batch']}...")
        
        for idx, ticket in enumerate(tickets_to_process, 1):
            issue_key = ticket['issue_key']
            combined_text = ticket['combined_text']
            has_existing_label = ticket['has_potential_ts_label']
            status = ticket['status']
            created = ticket['created'][:10] if ticket['created'] else 'Unknown'
            
            print(f"   [{idx}/{len(tickets_to_process)}] {issue_key} ({status}, {created}): ", end='', flush=True)
            
            # Check if ticket should be ignored based on description patterns or reporter
            if self.should_ignore_ticket(ticket['description'], ticket.get('reporter_email')):
                print(f"🚫 Ignored (automated notification)")
                self.save_processed_ticket(issue_key, "ignored_pattern", False)
                skipped_no_match += 1
                continue
            
            # Skip if already has the label
            if has_existing_label:
                print(f"⏭️  Already labeled")
                self.save_processed_ticket(issue_key, "already_labeled", False)
                skipped_already_labeled += 1
                continue
            
            # Check for keyword matches using fuzzy matching
            matched_keyword = self.fuzzy_check_keywords_match(combined_text)
            
            if not matched_keyword:
                print(f"❌ No keyword match")
                self.save_processed_ticket(issue_key, "no_match", False)
                skipped_no_match += 1
                continue
            
            # We have a match - add label
            if dry_run:
                print(f"✅ DRY RUN: Would label (matched: '{matched_keyword}')")
                self.save_processed_ticket(issue_key, matched_keyword, True)
                labeled_count += 1
            else:
                print(f"🏷️  Adding label (matched: '{matched_keyword}')...", end='', flush=True)
                success = self.add_potential_ts_label(issue_key)
                if success:
                    print(" ✅")
                    self.save_processed_ticket(issue_key, matched_keyword, True)
                    labeled_count += 1
                else:
                    print(" ❌ Failed")
                    self.save_processed_ticket(issue_key, matched_keyword, False)
                    failed_count += 1
            
            # Rate limiting
            if not dry_run:
                time.sleep(0.5)
        
        # Summary
        print("\n" + "=" * 80)
        print("📋 BATCH PROCESSING SUMMARY")
        print("=" * 80)
        print(f"   ✅ Newly labeled: {labeled_count}")
        print(f"   ⏭️  Skipped (already labeled): {skipped_already_labeled}")
        print(f"   🔍 Skipped (no keyword match): {skipped_no_match}")
        print(f"   ❌ Failed: {failed_count}")
        print(f"   📊 Total processed in this batch: {len(tickets_to_process)}")
        print(f"   💾 Total tracked in CSV: {len(self.processed_tickets)}")
        print(f"   📈 Overall progress: {self.scan_progress['total_processed']} tickets")
        print(f"   📅 Next batch will continue from: {self.scan_progress['last_processed_date']}")
        print("=" * 80 + "\n")
        
        return {
            'processed': len(tickets_to_process),
            'labeled': labeled_count,
            'skipped_already_labeled': skipped_already_labeled,
            'skipped_no_match': skipped_no_match,
            'failed': failed_count
        }


def main():
    """Main entry point"""
    import sys
    
    dry_run = '--dry-run' in sys.argv or '-d' in sys.argv
    
    # Parse batch size if provided
    batch_size = BATCH_SIZE
    for arg in sys.argv:
        if arg.startswith('--batch-size='):
            try:
                batch_size = int(arg.split('=')[1])
            except ValueError:
                print(f"⚠️  Invalid batch size: {arg}, using default: {BATCH_SIZE}")
        elif arg == '--batch-size' and len(sys.argv) > sys.argv.index(arg) + 1:
            try:
                batch_size = int(sys.argv[sys.argv.index(arg) + 1])
            except (ValueError, IndexError):
                print(f"⚠️  Invalid batch size, using default: {BATCH_SIZE}")
    
    # Check for reset flag
    reset_progress = '--reset' in sys.argv
    
    print(f"\n{'=' * 80}")
    print(f"📦 Batch size: {batch_size} tickets")
    print(f"📅 Default start date: {DEFAULT_START_DATE}")
    print(f"💾 Tracking processed tickets in: {PROCESSED_TICKETS_CSV}")
    print(f"📊 Progress tracking in: {PROGRESS_TRACKER_CSV}")
    print(f"🔍 Using fuzzy matching with {len(KEYWORDS)} keywords")
    print(f"🚫 Excluding tickets with 'Done' status")
    print(f"🚫 Excluding tickets with 'TS_SC' or 'script_cleared' labels")
    if reset_progress:
        print(f"🔄 RESET MODE - Progress will be reset to {DEFAULT_START_DATE}")
    print(f"{'=' * 80}\n")
    
    automation = OpsTicketLabelAutomation()
    
    # Reset progress if requested
    if reset_progress:
        print("🔄 Resetting progress tracker...")
        automation.scan_progress = {
            'last_processed_date': DEFAULT_START_DATE,
            'last_processed_ticket': None,
            'last_scan_date': None,
            'total_processed': 0,
            'current_batch': 0
        }
        automation.save_scan_progress(automation.scan_progress)
        print(f"✅ Progress reset to start date: {DEFAULT_START_DATE}\n")
    
    results = automation.process_tickets(dry_run=dry_run, batch_size=batch_size)
    
    if dry_run:
        print("\n💡 Run without --dry-run to actually add labels")
    
    # Show instructions for next run
    if not dry_run and results['processed'] > 0:
        print("💡 To continue scanning, run the script again without any parameters")
        print("💡 To reset progress and start over from October 1, 2025, use: --reset")


if __name__ == "__main__":
    main()