# Jira Auto Reply Agent

An automated Python agent that monitors Jira tickets in "To-Do" status and automatically replies, assigns them to a specified user, and transitions them through workflow states.

## Features

- 🔍 **Automated Ticket Monitoring**: Searches for unassigned tickets in "To-Do" status
- 🤖 **AI-Powered Replies**: Uses OpenAI API to generate contextual auto-replies (with fallback to default messages)
- 👤 **Auto-Assignment**: Automatically assigns tickets to a specified user
- 🔄 **Workflow Automation**: Transitions tickets through multiple statuses (To-Do → In Progress → Waiting for Client)
- ⏰ **Time Window Control**: Only processes tickets during specified hours (default: 5:30 AM - 5:30 PM)
- 📝 **Smart Tracking**: Tracks last run time to avoid reprocessing tickets

## Requirements

- Python 3.9+
- `uv` package manager (for virtual environment)
- Jira API token
- OpenAI API key (optional, falls back to default replies if not provided)

## Installation

### 1. Clone or navigate to the project directory

```powershell
cd C:\Users\anura\Downloads\Devlpoment\ai_agent
```

### 2. Create virtual environment using `uv`

```powershell
uv venv
```

### 3. Activate the virtual environment

**Windows PowerShell:**
```powershell
.venv\Scripts\activate
```

**Windows CMD:**
```cmd
.venv\Scripts\activate.bat
```

**Linux/Mac:**
```bash
source .venv/bin/activate
```

### 4. Install dependencies

```powershell
uv pip install -r requirements.txt
```

## Configuration

### Environment Variables

Create a `.env` file in the project root (or set environment variables) with the following:

```env
# Required Jira Configuration
JIRA_BASE_URL=https://certifyos.atlassian.net
JIRA_EMAIL=anurag.rai@certifyos.com
JIRA_API_TOKEN=your_jira_api_token_here
JIRA_ASSIGNEE_ACCOUNT_ID=712020:c0bede50-504a-4050-9cfb-663b18068ef0

# Project Configuration
PROJECT_KEY=TS
ASSIGNEE_FALLBACK=currentUser()
STATUS_FIELD_ID=customfield_10353

# Status Transitions (must match your Jira workflow transition names)
STATUS_TRANSITION_TODO_TO_IN_PROGRESS=To-Do
STATUS_TRANSITION_IN_PROGRESS_TO_WAITING=Iprogress
STATUS_TRANSITION_WAITING_TO_IN_PROGRESS=Wating for clinet

# Optional: OpenAI Configuration
OPENAI_API_KEY=your_openai_api_key_here
OPENAI_MODEL=gpt-4o-mini

# Optional: Time Window Configuration
TIMEZONE=Asia/Kolkata
WINDOW_START=05:30
WINDOW_END=17:30

# Optional: Confluence Configuration
USE_CONFLUENCE_KB=false
CONFLUENCE_BASE_URL=https://certifyos.atlassian.net/wiki
CONFLUENCE_SPACE_KEY=TS

# Optional: File Paths
LAST_RUN_PATH=last_run.json
```

### Setting Environment Variables in PowerShell

```powershell
$env:JIRA_BASE_URL="https://certifyos.atlassian.net"
$env:JIRA_EMAIL="anurag.rai@certifyos.com"
$env:JIRA_API_TOKEN="your_token"
$env:JIRA_ASSIGNEE_ACCOUNT_ID="712020:c0bede50-504a-4050-9cfb-663b18068ef0"
$env:PROJECT_KEY="TS"
# ... set other variables
```

### Getting Your Jira API Token

1. Go to [Atlassian Account Settings](https://id.atlassian.com/manage-profile/security/api-tokens)
2. Click "Create API token"
3. Copy the token and use it as `JIRA_API_TOKEN`

### Getting Your Jira Account ID

1. Go to your Jira profile
2. The Account ID is in the URL or profile settings
3. Format: `712020:c0bede50-504a-4050-9cfb-663b18068ef0`

## Usage

### Running the Script

```powershell
python jira_auto_reply.py
```

### Running in Jupyter Notebook

Open `jira_auto_reply.ipynb` in Jupyter and run the cells. Make sure to set environment variables before running.

### What It Does

1. **Checks Time Window**: Only runs between configured start and end times
2. **Searches Tickets**: Finds unassigned tickets in "To-Do" status updated since last run
3. **Generates Reply**: Creates an AI-powered or default reply message
4. **Adds Comment**: Posts the reply as a comment on the ticket
5. **Assigns Ticket**: Assigns the ticket to the configured user
6. **Transitions Status**: Moves ticket through workflow states:
   - To-Do → In Progress
   - In Progress → Waiting for Client

## Testing

Run the test suite:

```powershell
pytest tests/test_jira_auto_reply.py -v
```

Run with coverage:

```powershell
pytest tests/test_jira_auto_reply.py --cov=jira_auto_reply --cov-report=html
```

### Test Coverage

The test suite covers:
- ✅ Configuration loading and validation
- ✅ Time window logic (same-day and overnight windows)
- ✅ Jira API interactions (search, comment, assign, transition)
- ✅ OpenAI integration with fallback
- ✅ End-to-end process flow

## Project Structure

```
ai_agent/
├── jira_auto_reply.py          # Main script
├── jira_auto_reply.ipynb       # Jupyter notebook version
├── requirements.txt            # Python dependencies
├── README.md                   # This file
├── last_run.json              # Last run timestamp (auto-generated)
├── tests/
│   ├── conftest.py            # Pytest configuration
│   └── test_jira_auto_reply.py # Test suite
└── .venv/                     # Virtual environment (created by uv)
```

## Scheduling (Optional)

### Windows Task Scheduler

1. Open Task Scheduler
2. Create Basic Task
3. Set trigger (e.g., daily at 5:30 AM)
4. Set action to run: `python C:\Users\anura\Downloads\Devlpoment\ai_agent\jira_auto_reply.py`
5. Set "Start in" directory to project folder

### Cron (Linux/Mac)

Add to crontab (`crontab -e`):

```bash
30 5 * * * cd /path/to/ai_agent && .venv/bin/python jira_auto_reply.py
```

## Troubleshooting

### Import Errors

If you see `ModuleNotFoundError`, ensure:
- Virtual environment is activated
- Dependencies are installed: `uv pip install -r requirements.txt`
- You're running from the project root directory

### Jira API Errors

- Verify your API token is correct
- Check that your account has permissions to:
  - Search tickets
  - Add comments
  - Assign tickets
  - Transition tickets
- Ensure transition names match your workflow exactly

### Time Window Issues

- Check your timezone setting matches your local timezone
- Verify time format is `HH:MM` (24-hour format)
- The script exits silently if outside the time window

## Dependencies

- `requests` - HTTP library for API calls
- `pytest` - Testing framework
- `pytest-mock` - Mocking utilities for tests

## License

This project is for internal use at CertifyOS.

## Implementation Guide

For a detailed step-by-step implementation path, see [IMPLEMENTATION_PATH.md](IMPLEMENTATION_PATH.md). This document covers:
- Setup and configuration phases
- Testing and validation procedures
- Deployment options (manual, Task Scheduler, cloud)
- Monitoring and maintenance guidelines
- Future optimization opportunities

## Support

For issues or questions, contact: anurag.rai@certifyos.com
