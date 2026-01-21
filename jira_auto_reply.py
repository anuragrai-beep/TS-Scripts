import json
import os
import sys
from dataclasses import dataclass
from datetime import datetime, time, timedelta
from typing import Any, Dict, Iterable, List, Optional

import requests

try:
    from zoneinfo import ZoneInfo
except ImportError:  # Python < 3.9
    ZoneInfo = None  # type: ignore


@dataclass(frozen=True)
class Config:
    jira_base_url: str
    jira_email: str
    jira_api_token: str
    jira_assignee_account_id: str
    project_key: str
    assignee_fallback: str
    status_field_id: str
    todo_transition: str
    in_progress_transition: str
    waiting_transition: str
    use_confluence_kb: bool
    confluence_base_url: str
    confluence_space_key: str
    openai_api_key: Optional[str]
    openai_model: str
    timezone: Optional[str]
    window_start: time
    window_end: time
    last_run_path: str


def load_config() -> Config:
    def get_env(name: str, default: Optional[str] = None) -> str:
        value = os.getenv(name, default)
        if value is None:
            raise ValueError(f"Missing required environment variable: {name}")
        return value

    timezone = os.getenv("TIMEZONE")

    window_start = parse_time_env("WINDOW_START", "05:30")
    window_end = parse_time_env("WINDOW_END", "17:30")

    return Config(
        jira_base_url=get_env("JIRA_BASE_URL"),
        jira_email=get_env("JIRA_EMAIL"),
        jira_api_token=get_env("JIRA_API_TOKEN"),
        jira_assignee_account_id=get_env("JIRA_ASSIGNEE_ACCOUNT_ID"),
        project_key=get_env("PROJECT_KEY", "TS"),
        assignee_fallback=get_env("ASSIGNEE_FALLBACK", "currentUser()"),
        status_field_id=get_env("STATUS_FIELD_ID", "customfield_10353"),
        todo_transition=get_env("STATUS_TRANSITION_TODO_TO_IN_PROGRESS", "To-Do"),
        in_progress_transition=get_env("STATUS_TRANSITION_IN_PROGRESS_TO_WAITING", "Iprogress"),
        waiting_transition=get_env("STATUS_TRANSITION_WAITING_TO_IN_PROGRESS", "Wating for clinet"),
        use_confluence_kb=get_env("USE_CONFLUENCE_KB", "false").lower() == "true",
        confluence_base_url=get_env("CONFLUENCE_BASE_URL", "https://certifyos.atlassian.net/wiki"),
        confluence_space_key=get_env("CONFLUENCE_SPACE_KEY", "TS"),
        openai_api_key=os.getenv("OPENAI_API_KEY"),
        openai_model=get_env("OPENAI_MODEL", "gpt-4o-mini"),
        timezone=timezone,
        window_start=window_start,
        window_end=window_end,
        last_run_path=get_env("LAST_RUN_PATH", "last_run.json"),
    )


def parse_time_env(env_name: str, default_value: str) -> time:
    raw_value = os.getenv(env_name, default_value)
    try:
        hours, minutes = raw_value.split(":")
        return time(int(hours), int(minutes))
    except Exception as exc:
        raise ValueError(f"Invalid time value for {env_name}: {raw_value}") from exc


def now_local(config: Config) -> datetime:
    if config.timezone and ZoneInfo:
        return datetime.now(ZoneInfo(config.timezone))
    return datetime.now()


def within_reply_window(config: Config, now: datetime) -> bool:
    start = config.window_start
    end = config.window_end
    current = now.time()
    if start <= end:
        return start <= current <= end
    return current >= start or current <= end


def load_last_run(path: str) -> datetime:
    if not os.path.exists(path):
        return datetime.now() - timedelta(days=1)
    try:
        with open(path, "r", encoding="utf-8") as handle:
            payload = json.load(handle)
        return datetime.fromisoformat(payload["last_run"])
    except Exception:
        return datetime.now() - timedelta(days=1)


def save_last_run(path: str, timestamp: datetime) -> None:
    with open(path, "w", encoding="utf-8") as handle:
        json.dump({"last_run": timestamp.isoformat()}, handle)


def jira_headers() -> Dict[str, str]:
    return {"Accept": "application/json", "Content-Type": "application/json"}


def jira_auth(config: Config) -> tuple:
    return (config.jira_email, config.jira_api_token)


def jira_search_issues(config: Config, last_run: datetime) -> List[Dict[str, Any]]:
    jql = (
        f'project = "{config.project_key}" '
        f'AND assignee is EMPTY '
        f'AND status = "To-Do" '
        f'AND statusCategory != Done '
        f'AND updated >= "{last_run.isoformat()}" '
        f'ORDER BY created ASC'
    )
    response = requests.get(
        f"{config.jira_base_url}/rest/api/3/search/jql",
        headers=jira_headers(),
        auth=jira_auth(config),
        params={"jql": jql},
        timeout=30,
    )
    response.raise_for_status()
    data = response.json()
    return data.get("issues", [])


def jira_add_comment(config: Config, issue_key: str, body: str) -> None:
    payload = {"body": body}
    response = requests.post(
        f"{config.jira_base_url}/rest/api/3/issue/{issue_key}/comment",
        headers=jira_headers(),
        auth=jira_auth(config),
        json=payload,
        timeout=30,
    )
    response.raise_for_status()


def jira_assign_issue(config: Config, issue_key: str, assignee_account_id: str) -> None:
    payload = {"accountId": assignee_account_id}
    response = requests.put(
        f"{config.jira_base_url}/rest/api/3/issue/{issue_key}/assignee",
        headers=jira_headers(),
        auth=jira_auth(config),
        json=payload,
        timeout=30,
    )
    response.raise_for_status()


def jira_list_transitions(config: Config, issue_key: str) -> List[Dict[str, Any]]:
    response = requests.get(
        f"{config.jira_base_url}/rest/api/3/issue/{issue_key}/transitions",
        headers=jira_headers(),
        auth=jira_auth(config),
        timeout=30,
    )
    response.raise_for_status()
    data = response.json()
    return data.get("transitions", [])


def jira_transition_issue(config: Config, issue_key: str, transition_name: str) -> None:
    transitions = jira_list_transitions(config, issue_key)
    transition = next(
        (item for item in transitions if item.get("name") == transition_name), None
    )
    if not transition:
        raise ValueError(f"Transition '{transition_name}' not found for {issue_key}")
    response = requests.post(
        f"{config.jira_base_url}/rest/api/3/issue/{issue_key}/transitions",
        headers=jira_headers(),
        auth=jira_auth(config),
        json={"transition": {"id": transition["id"]}},
        timeout=30,
    )
    response.raise_for_status()


def build_default_reply(issue: Dict[str, Any]) -> str:
    summary = issue.get("fields", {}).get("summary", "your ticket")
    return (
        f"Thanks for reaching out! We received **{summary}**. "
        "Our team will review this shortly and respond with next steps."
    )


def generate_ai_reply(config: Config, issue: Dict[str, Any]) -> str:
    if not config.openai_api_key:
        return build_default_reply(issue)

    summary = issue.get("fields", {}).get("summary", "")
    description = issue.get("fields", {}).get("description", "")
    prompt = (
        "You are an assistant for support tickets. Draft a concise, friendly "
        "auto-reply acknowledging the ticket and asking for any missing info. "
        "Keep it under 80 words.\n\n"
        f"Summary: {summary}\n"
        f"Description: {description}"
    )

    response = requests.post(
        "https://api.openai.com/v1/chat/completions",
        headers={
            "Authorization": f"Bearer {config.openai_api_key}",
            "Content-Type": "application/json",
        },
        json={
            "model": config.openai_model,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.3,
        },
        timeout=30,
    )
    response.raise_for_status()
    data = response.json()
    return data["choices"][0]["message"]["content"].strip()


def process_issues(
    config: Config, issues: Iterable[Dict[str, Any]], assignee_account_id: str
) -> int:
    count = 0
    for issue in issues:
        issue_key = issue.get("key")
        if not issue_key:
            continue
        reply = generate_ai_reply(config, issue)
        jira_add_comment(config, issue_key, reply)
        jira_assign_issue(config, issue_key, assignee_account_id)
        jira_transition_issue(config, issue_key, config.todo_transition)
        jira_transition_issue(config, issue_key, config.in_progress_transition)
        jira_transition_issue(config, issue_key, config.waiting_transition)
        count += 1
    return count


def main() -> int:
    try:
        config = load_config()
    except ValueError as exc:
        print(str(exc))
        return 1

    now = now_local(config)
    if not within_reply_window(config, now):
        print("Outside reply window. Exiting.")
        return 0

    last_run = load_last_run(config.last_run_path)
    issues = jira_search_issues(config, last_run)
    if not issues:
        print("No matching issues found.")
        save_last_run(config.last_run_path, now)
        return 0

    processed = process_issues(config, issues, config.jira_assignee_account_id)
    save_last_run(config.last_run_path, now)
    print(f"Processed {processed} issues.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
