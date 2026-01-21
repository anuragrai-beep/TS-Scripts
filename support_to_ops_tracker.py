#!/usr/bin/env python3
"""
Support to Operations Tracker: Track tickets moved from Support Ticket to Operations Ticket
This script:
1. Finds all TS issues and extracts issue type changes from Support Ticket to Operations Ticket
2. Generates monthly reports for issues that are currently Operations Tickets
3. Only processes the past 2 months
4. No label addition (reports only)
"""

import os
import pandas as pd
import json
import logging
import time
import requests
from typing import List, Dict, Optional
from datetime import datetime

# Import necessary components from consolidated script
from consolidated_ops_to_support_tracker import (
    ConsolidatedOpsToSupportTracker,
    TimestampFormatter,
    JIRA_URL
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


class SupportToOpsTracker(ConsolidatedOpsToSupportTracker):
    """Extended tracker for Support Ticket → Operations Ticket changes"""
    
    def __init__(self, project_key: str = "TS", base_date: str = "2025-10-24"):
        """Initialize with reversed logic"""
        super().__init__(project_key, base_date)
        
        # Override file names to indicate reversed direction (using JQL-based naming)
        self.issues_file = f"{project_key.lower()}_issues_jql_past_2months.csv"
        self.changes_file = f"{project_key.lower()}_support_to_ops_changes_jql.csv"
    
    def extract_issue_type_changes(self, issue_key: str, histories: List[Dict]) -> List[Dict]:
        """Extract issue type changes from Support Ticket to Operations Ticket (REVERSED)"""
        changes = []
        
        for history in histories:
            for item in history.get('items', []):
                if item.get('field') == 'issuetype':
                    from_type = item.get('fromString', '')
                    to_type = item.get('toString', '')
                    
                    # REVERSED: Check if this is a change from Support Ticket to Operations Ticket
                    if from_type == 'Support Ticket' and to_type == 'Operations Ticket':
                        changes.append({
                            'issue_key': issue_key,
                            'change_timestamp': history.get('created'),
                            'change_author': history.get('author', {}).get('displayName', 'Unknown'),
                            'from_type': from_type,
                            'to_type': to_type
                        })
        
        return changes
    
    def fetch_issues_with_jql(self, jql_query: str = None) -> List[Dict]:
        """
        Fetch issues using JQL via POST /rest/api/3/search/jql.
        Default: project = "<key>" AND updated >= -60d (last ~2 months of activity)
        """
        if jql_query is None:
            jql_query = f'project = "{self.project_key}" AND updated >= -60d'

        print("=" * 80)
        print("STEP 1: FETCHING ISSUES WITH JQL")
        print("=" * 80)
        print(f"🔍 Executing JQL Query:\n   {jql_query}")

        url = f"{JIRA_URL}/rest/api/3/search/jql"
        all_issues: List[Dict] = []
        max_results = 100
        next_page_token: Optional[str] = None
        page = 0

        # Fields we want on each issue
        fields_list = [
            "summary",
            "issuetype",
            "status",
            "created",
            "updated",
            "assignee",
            "reporter",
            "customfield_10024",  # date of first response
            "comment",
        ]

        while True:
            payload: Dict = {
                "jql": jql_query,
                "maxResults": max_results,
                "fields": fields_list,
                # expand is a STRING, not a list
                "expand": "changelog",
            }
            if next_page_token:
                payload["nextPageToken"] = next_page_token

            try:
                response = self.session.post(url, json=payload, timeout=45)
                print(f"   HTTP status: {response.status_code}")
                response.raise_for_status()
                data = response.json()

                # /search/jql uses SearchAndReconcileResults
                issues = data.get("issues", []) or []
                is_last = data.get("isLast", True)
                next_page_token = data.get("nextPageToken")

                all_issues.extend(issues)
                page += 1
                print(f"   Page {page}: fetched {len(issues)} issues "
                    f"(total so far: {len(all_issues)})")

                if is_last or not issues or not next_page_token:
                    break

            except requests.exceptions.HTTPError as e:
                print(f"   ❌ HTTP Error fetching issues from Jira: {e}")
                if hasattr(e, "response") and e.response is not None:
                    print(f"   Response Body: {e.response.text[:500]}")
                # On error, bail out and return whatever we have (likely empty)
                return all_issues
            except requests.exceptions.RequestException as e:
                print(f"   ❌ Network or Request Error fetching issues from Jira: {e}")
                return all_issues

        print(f"\n✅ Successfully fetched details for {len(all_issues)} issues.")
        return all_issues


    
    def find_all_issues_and_changes_with_jql(self, jql_query: str = None):
        """
        Find all issues using JQL and extract issue type changes from Support Ticket to Operations Ticket
        """
        print("=" * 80)
        print("STEP 1: FINDING ISSUES AND EXTRACTING CHANGES (USING JQL)")
        print("=" * 80)
        
        # Fetch issues using JQL
        all_issues_data = self.fetch_issues_with_jql(jql_query)
        
        if not all_issues_data:
            print("⚠️  No issues found. Creating empty files.")
            # Create empty DataFrames
            df_issues = pd.DataFrame(columns=['key', 'summary', 'issue_type', 'status', 'created', 
                                             'updated', 'assignee', 'reporter', 'date_of_first_response'])
            df_changes = pd.DataFrame(columns=['issue_key', 'change_timestamp', 'change_author', 
                                              'from_type', 'to_type'])
            df_issues.to_csv(self.issues_file, index=False)
            df_changes.to_csv(self.changes_file, index=False)
            return df_issues, df_changes
        
        all_issues = []
        all_changes = []
        
        print(f"\n📊 Processing {len(all_issues_data)} issues to extract changes...")
        
        for issue_data in all_issues_data:
            fields = issue_data.get('fields', {})
            issue_key = issue_data.get('key', '')
            
            # Get Date of First Response - prioritize comments from allowed responders
            first_response_info = self.get_first_response_from_allowed_responder(issue_data)
            
            if first_response_info:
                date_of_first_response = first_response_info[0]
            else:
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
            
            if len(all_issues) % 100 == 0:
                print(f"   Progress: {len(all_issues)} issues processed, {len(all_changes)} changes found")
        
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
    
    def generate_monthly_reports(self, target_month: Optional[str] = None, months: int = 2):
        """Generate monthly reports for issues moved from Support to Ops (last N months)"""
        print("\n" + "=" * 80)
        print("STEP 2: GENERATING MONTHLY REPORTS (SUPPORT → OPS)")
        print("=" * 80)
        
        # Load data
        print(f"📊 Loading data from CSV files...")
        if not os.path.exists(self.issues_file):
            print(f"   ❌ Issues file not found: {self.issues_file}")
            print(f"   💡 Run with --skip-fetch=false first to fetch issues")
            return []
        
        if not os.path.exists(self.changes_file):
            print(f"   ❌ Changes file not found: {self.changes_file}")
            print(f"   💡 Run with --skip-fetch=false first to fetch changes")
            return []
        
        df_issues = pd.read_csv(self.issues_file)
        df_changes = pd.read_csv(self.changes_file)
        
        print(f"   ✅ Loaded {len(df_issues)} issues")
        print(f"   ✅ Loaded {len(df_changes)} changes")
        
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
            # Process last N months (default: 2)
            months_to_process = available_months.index[-months:].tolist()
            print(f"\n🎯 Processing last {months} months: {', '.join(str(m) for m in months_to_process)}")
        
        generated_reports = []
        
        for year_month in months_to_process:
            report_file = self.generate_single_month_report(df_issues, df_changes, year_month)
            if report_file:
                generated_reports.append(report_file)
        
        return generated_reports
    
    def generate_single_month_report(self, df_issues: pd.DataFrame, 
                                    df_changes: pd.DataFrame, 
                                    year_month: pd.Period) -> Optional[str]:
        """Generate report for a single month (REVERSED logic)"""
        year_month_str = str(year_month)
        print(f"\n{'=' * 60}")
        print(f"GENERATING REPORT FOR {year_month_str} (SUPPORT → OPS)")
        print(f"{'=' * 60}")
        
        # Filter changes for this month
        monthly_changes = df_changes[df_changes['year_month'] == year_month].copy()
        print(f"✅ Found {len(monthly_changes)} issue type changes for {year_month_str}")
        
        # Create detailed report with filtering (REVERSED)
        report_data = []
        
        for _, change in monthly_changes.iterrows():
            issue_key = change['issue_key']
            issue_details = df_issues[df_issues['key'] == issue_key]
            
            if len(issue_details) > 0:
                issue = issue_details.iloc[0]
                
                # REVERSED: Only include if current issue type is "Operations Ticket" 
                # and it was changed from "Support Ticket" to "Operations Ticket"
                if (issue['issue_type'] == 'Operations Ticket' and 
                    change['from_type'] == 'Support Ticket' and 
                    change['to_type'] == 'Operations Ticket'):
                    
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
        
        # Save report with reversed naming
        report_file = f"{self.project_key.lower()}_support_to_ops_changes_{year_month_str.replace('-', '_')}.csv"
        report_df.to_csv(report_file, index=False)
        print(f"✅ Report saved to {report_file}")
        
        # Generate summary
        summary = self.generate_summary(report_df, year_month_str)
        
        # Save summary
        summary_file = f"{self.project_key.lower()}_support_to_ops_summary_{year_month_str.replace('-', '_')}.json"
        with open(summary_file, 'w') as f:
            json.dump(summary, f, indent=2, default=str)
        print(f"✅ Summary saved to {summary_file}")
        
        # Print summary
        self.print_summary(summary, year_month_str)
        
        return report_file
    
    def run_full_process(self, skip_fetch: bool = False, target_month: Optional[str] = None,
                         months: int = 2, jql_query: str = None):
        """Run the complete process: find and report (NO LABEL ADDITION)"""
        print("\n" + "=" * 80)
        print("🚀 SUPPORT TO OPERATIONS TRACKER")
        print("=" * 80)
        print(f"Project: {self.project_key}")
        print(f"Target: Issues moved from Support Ticket to Operations Ticket")
        print(f"Report Period: Last {months} months")
        print(f"JQL Query: {jql_query or f'project = {self.project_key} AND created >= -8w'}")
        print("=" * 80)
        
        start_time = time.time()
        
        # Step 1: Find issues and changes using JQL (can be skipped if files exist)
        if skip_fetch and os.path.exists(self.issues_file) and os.path.exists(self.changes_file):
            print(f"\n⏭️  Skipping Step 1 - Using existing files:")
            print(f"   - {self.issues_file}")
            print(f"   - {self.changes_file}")
        else:
            print(f"\n📥 Step 1: Fetching issues using JQL and extracting changes...")
            self.find_all_issues_and_changes_with_jql(jql_query)
        
        # Step 2: Generate monthly reports (last 2 months)
        print(f"\n📊 Step 2: Generating monthly reports for last {months} months...")
        report_files = self.generate_monthly_reports(target_month, months=months)
        
        # Step 3: SKIPPED - No label addition for this tracker
        
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
        description='Track tickets moved from Support Ticket to Operations Ticket (last 2 months)'
    )
    parser.add_argument(
        '--skip-fetch',
        action='store_true',
        help='Skip fetching issues from Jira (use existing CSV files)'
    )
    parser.add_argument(
        '--jql',
        type=str,
        help='Custom JQL query (default: project = TS AND created >= -8w)'
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
        help='Base date (kept for compatibility, not used with JQL)'
    )
    parser.add_argument(
        '--months',
        type=int,
        default=2,
        help='Number of months to process (default: 2)'
    )
    parser.add_argument(
        '--test',
        action='store_true',
        help='Test mode: Try simpler queries to verify API connection'
    )
    
    args = parser.parse_args()
    
    # Test mode: Try different queries to verify API works
    if args.test:
        print("=" * 80)
        print("🧪 TEST MODE: Testing API connection with different queries")
        print("=" * 80)
        
        tracker = SupportToOpsTracker(project_key=args.project, base_date=args.base_date)
        
        test_queries = [
            f'project = "{args.project}"',
            f'project = {args.project}',
            f'project = "{args.project}" AND created >= -30d',
            f'project = "{args.project}" AND created >= -8w',
            f'project = "{args.project}" ORDER BY created DESC',
        ]
        
        for query in test_queries:
            print(f"\n🔍 Testing query: {query}")
            issues = tracker.fetch_issues_with_jql(query)
            if issues:
                print(f"   ✅ Found {len(issues)} issues!")
                if len(issues) > 0:
                    print(f"   Sample issue: {issues[0].get('key', 'N/A')}")
                break
            else:
                print(f"   ⚠️  No issues found")
        
        print("\n" + "=" * 80)
        print("🧪 Test mode complete")
        print("=" * 80)
        return
    
    # Create tracker instance
    tracker = SupportToOpsTracker(project_key=args.project, base_date=args.base_date)
    
    # Note: base_range and weekly_increment are not used with JQL approach
    # but kept for compatibility
    
    # Run the full process (no label addition)
    tracker.run_full_process(
        skip_fetch=args.skip_fetch, 
        target_month=args.month, 
        months=args.months,
        jql_query=args.jql
    )


if __name__ == "__main__":
    main()

