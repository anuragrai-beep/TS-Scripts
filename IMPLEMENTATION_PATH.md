# Implementation Path - Jira Auto Reply Agent

This document outlines the step-by-step implementation path for deploying the Jira Auto Reply Agent.

## Phase 1: Initial Setup & Configuration (Day 1)

### 1.1 Environment Setup
- [x] Create project directory
- [x] Set up virtual environment using `uv`
- [x] Install dependencies (`requests`, `pytest`, `pytest-mock`)
- [x] Verify Python 3.9+ is installed

### 1.2 Jira Configuration
- [ ] **Obtain Jira API Token**
  - Go to: https://id.atlassian.com/manage-profile/security/api-tokens
  - Create new API token
  - Save securely (cannot be retrieved later)

- [ ] **Get Account ID**
  - Account ID: `712020:c0bede50-504a-4050-9cfb-663b18068ef0` (already known)
  - Verify account has permissions:
    - Browse projects
    - Create comments
    - Assign issues
    - Transition issues

- [ ] **Verify Jira Workflow**
  - Confirm transition names match exactly:
    - "To-Do" → "Iprogress" → "Wating for clinet"
  - Test manual transitions to verify names

### 1.3 OpenAI Configuration (Optional)
- [ ] **Obtain OpenAI API Key**
  - Go to: https://platform.openai.com/api-keys
  - Create new API key
  - Set usage limits/billing alerts
  - Save securely

- [ ] **Test OpenAI Connection**
  - Run a test API call to verify key works
  - Check rate limits and quotas

## Phase 2: Testing & Validation (Day 2-3)

### 2.1 Unit Testing
- [x] Run test suite: `pytest tests/test_jira_auto_reply.py -v`
- [x] Verify all 9 tests pass
- [ ] Review test coverage
- [ ] Add additional edge case tests if needed

### 2.2 Configuration Testing
- [ ] **Set Environment Variables**
  ```powershell
  $env:JIRA_BASE_URL="https://certifyos.atlassian.net"
  $env:JIRA_EMAIL="anurag.rai@certifyos.com"
  $env:JIRA_API_TOKEN="your_token"
  $env:JIRA_ASSIGNEE_ACCOUNT_ID="712020:c0bede50-504a-4050-9cfb-663b18068ef0"
  $env:PROJECT_KEY="TS"
  $env:STATUS_TRANSITION_TODO_TO_IN_PROGRESS="To-Do"
  $env:STATUS_TRANSITION_IN_PROGRESS_TO_WAITING="Iprogress"
  $env:STATUS_TRANSITION_WAITING_TO_IN_PROGRESS="Wating for clinet"
  ```

- [ ] **Test Configuration Loading**
  - Run script with missing variables (should error)
  - Run script with all variables (should load successfully)

### 2.3 Dry Run Testing
- [ ] **Create Test Ticket**
  - Create a test ticket in TS project
  - Set status to "To-Do"
  - Leave unassigned
  - Note ticket key (e.g., TS-123)

- [ ] **Run Script Manually**
  ```powershell
  python jira_auto_reply.py
  ```
  
- [ ] **Verify Actions**
  - [ ] Comment was added
  - [ ] Ticket was assigned to correct user
  - [ ] Status transitions occurred correctly
  - [ ] No errors in console

- [ ] **Check Time Window**
  - Test outside time window (should exit silently)
  - Test inside time window (should process)

### 2.4 Error Handling Testing
- [ ] Test with invalid API token (should fail gracefully)
- [ ] Test with non-existent transition name (should error clearly)
- [ ] Test with network issues (should handle timeout)
- [ ] Test with OpenAI API failure (should fallback to default reply)

## Phase 3: Production Preparation (Day 4)

### 3.1 Security Hardening
- [ ] **Secure Credential Storage**
  - Option A: Use Windows Credential Manager
  - Option B: Use environment variables (not in code)
  - Option C: Use `.env` file with `.gitignore` (if using Git)
  - **NEVER commit API keys to version control**

- [ ] **Create `.env.example`** (template without secrets)
  ```env
  JIRA_BASE_URL=https://certifyos.atlassian.net
  JIRA_EMAIL=your_email@certifyos.com
  JIRA_API_TOKEN=your_token_here
  JIRA_ASSIGNEE_ACCOUNT_ID=your_account_id
  # ... etc
  ```

- [ ] **Add `.env` to `.gitignore`** (if using Git)
  ```
  .env
  last_run.json
  __pycache__/
  .pytest_cache/
  .venv/
  ```

### 3.2 Logging & Monitoring
- [ ] **Add Logging** (optional enhancement)
  - Log each processed ticket
  - Log errors with details
  - Log time window checks
  - Save logs to file for audit trail

- [ ] **Set Up Alerts** (optional)
  - Email notifications on errors
  - Slack/Teams webhook for status updates

### 3.3 Documentation
- [x] README.md created
- [x] Implementation path document (this file)
- [ ] Create runbook for operations team
- [ ] Document troubleshooting procedures

## Phase 4: Deployment Options

### Option A: Manual Execution (Recommended for Start)
**When to use:** Testing phase, low volume, ad-hoc runs

**Steps:**
1. Activate virtual environment
2. Set environment variables
3. Run script manually: `python jira_auto_reply.py`
4. Check output for processed tickets

**Pros:**
- Simple to set up
- Easy to debug
- Full control over execution

**Cons:**
- Requires manual intervention
- Easy to forget to run

### Option B: Windows Task Scheduler (Recommended for Production)
**When to use:** Production, automated runs, Windows server

**Steps:**
1. Open Task Scheduler (`taskschd.msc`)
2. Create Basic Task
3. **General Tab:**
   - Name: "Jira Auto Reply Agent"
   - Description: "Automatically replies to Jira tickets"
   - Run whether user is logged on: Yes
   - Run with highest privileges: Yes

4. **Trigger Tab:**
   - Daily at 5:30 AM
   - Repeat every 1 hour until 5:30 PM
   - Enabled: Yes

5. **Action Tab:**
   - Action: Start a program
   - Program/script: `C:\Users\anura\Downloads\Devlpoment\ai_agent\.venv\Scripts\python.exe`
   - Arguments: `jira_auto_reply.py`
   - Start in: `C:\Users\anura\Downloads\Devlpoment\ai_agent`

6. **Conditions Tab:**
   - Uncheck "Start the task only if the computer is on AC power"
   - Check "Wake the computer to run this task" (if needed)

7. **Settings Tab:**
   - Allow task to be run on demand: Yes
   - Run task as soon as possible after a scheduled start is missed: Yes
   - If the task fails, restart every: 10 minutes (max 3 times)

**Environment Variables Setup:**
- Create a batch file `run_jira_agent.bat`:
  ```batch
  @echo off
  set JIRA_BASE_URL=https://certifyos.atlassian.net
  set JIRA_EMAIL=anurag.rai@certifyos.com
  set JIRA_API_TOKEN=your_token
  set JIRA_ASSIGNEE_ACCOUNT_ID=712020:c0bede50-504a-4050-9cfb-663b18068ef0
  set PROJECT_KEY=TS
  REM ... add other variables
  
  cd C:\Users\anura\Downloads\Devlpoment\ai_agent
  .venv\Scripts\python.exe jira_auto_reply.py
  ```

- Update Task Scheduler action to run the batch file instead

### Option C: Python Script as Windows Service
**When to use:** Always-on service, background execution

**Tools needed:**
- `pywin32` or `NSSM` (Non-Sucking Service Manager)

**Steps:**
1. Install NSSM: https://nssm.cc/download
2. Create service wrapper script
3. Install as Windows service
4. Configure to start automatically

### Option D: Cloud Deployment (Future)
**When to use:** Scalability, reliability, cloud infrastructure

**Options:**
- **AWS Lambda** (serverless, pay per execution)
- **Azure Functions** (serverless)
- **Google Cloud Functions** (serverless)
- **Docker Container** on cloud VM (more control)

## Phase 5: Monitoring & Maintenance (Ongoing)

### 5.1 Daily Checks (First Week)
- [ ] Verify script ran successfully
- [ ] Check processed ticket count
- [ ] Review any error logs
- [ ] Verify ticket assignments and transitions

### 5.2 Weekly Reviews
- [ ] Review ticket processing patterns
- [ ] Check for any failed transitions
- [ ] Verify time window is appropriate
- [ ] Review OpenAI usage/costs (if applicable)

### 5.3 Monthly Maintenance
- [ ] Rotate API tokens (security best practice)
- [ ] Review and update transition names if workflow changes
- [ ] Check for Jira API updates/deprecations
- [ ] Review and optimize reply quality

### 5.4 Troubleshooting Checklist
- [ ] Script not running: Check Task Scheduler status
- [ ] No tickets processed: Verify JQL query, check time window
- [ ] Assignment failures: Verify account ID is correct
- [ ] Transition failures: Verify transition names match workflow
- [ ] API errors: Check token validity, network connectivity

## Phase 6: Optimization & Enhancement (Future)

### Potential Enhancements
- [ ] **Confluence KB Integration**
  - Search knowledge base for similar tickets
  - Include KB links in replies

- [ ] **Smart Reply Customization**
  - Category-based reply templates
  - Priority-based response urgency
  - Customer-specific tone adjustments

- [ ] **Advanced Filtering**
  - Filter by priority
  - Filter by component
  - Filter by custom fields

- [ ] **Reporting Dashboard**
  - Track response times
  - Monitor ticket volume
  - Generate weekly/monthly reports

- [ ] **Multi-Project Support**
  - Configure multiple projects
  - Different workflows per project

- [ ] **Error Recovery**
  - Retry failed operations
  - Queue system for high volume
  - Dead letter queue for failures

## Implementation Timeline

| Phase | Duration | Status |
|-------|----------|--------|
| Phase 1: Setup & Configuration | 1 day | ✅ Complete |
| Phase 2: Testing & Validation | 2-3 days | 🔄 In Progress |
| Phase 3: Production Preparation | 1 day | ⏳ Pending |
| Phase 4: Deployment | 1 day | ⏳ Pending |
| Phase 5: Monitoring | Ongoing | ⏳ Pending |
| Phase 6: Optimization | As needed | ⏳ Future |

## Success Criteria

✅ **Phase 1 Complete When:**
- Virtual environment created
- Dependencies installed
- All credentials obtained

✅ **Phase 2 Complete When:**
- All unit tests pass
- Manual test run successful
- Error handling verified

✅ **Phase 3 Complete When:**
- Credentials secured
- Logging implemented (optional)
- Documentation complete

✅ **Phase 4 Complete When:**
- Automated scheduling configured
- Script runs successfully on schedule
- No manual intervention needed

✅ **Phase 5 Complete When:**
- Monitoring in place
- No critical errors for 1 week
- Tickets processing correctly

## Next Steps

1. **Immediate:** Complete Phase 2 testing
2. **Short-term:** Deploy using Windows Task Scheduler (Option B)
3. **Medium-term:** Add logging and monitoring
4. **Long-term:** Consider cloud deployment for scalability

## Support & Escalation

- **Technical Issues:** Review troubleshooting section in README
- **Jira API Issues:** Check Atlassian status page
- **OpenAI Issues:** Check OpenAI status page
- **Critical Failures:** Manual intervention required

---

**Last Updated:** 2025-01-XX
**Maintained By:** anurag.rai@certifyos.com
