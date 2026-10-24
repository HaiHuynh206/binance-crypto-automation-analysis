"""Create one GitHub issue per file in issue/NN-*.md (idempotent).

- md in the repo stays the source of truth; the GitHub issue links back to it.
- Skips files whose issue already exists (matched by the "[NN]" title prefix).
- Creates p1/p2/p3 labels if missing.

Run: python issue/sync/sync_issues.py   (or anywhere; uses ~/.git-credentials token)
"""
from __future__ import annotations

import json
import os
import pathlib
import re
import urllib.error
import urllib.parse
import urllib.request

REPO = "HaiHuynh206/binance-crypto-automation-analysis"
BRANCH = "main"
API = "https://api.github.com"
ROOT = pathlib.Path(__file__).resolve().parents[2]
ISSUE_DIR = ROOT / "issue"

LABELS = {
    "p1": ("d73a4a", "Chặn nộp bài — phải sửa"),
    "p2": ("fbca04", "Nên sửa — dễ sai khi chạy lại"),
    "p3": ("ededed", "Vệ sinh repo — không đổi số liệu"),
}

FM_RE = re.compile(r"^---\n(.*?)\n---\n", re.S)
LINK_RE = re.compile(r"\]\((?!https?://|#)([^)]+)\)")


def load_token() -> str:
    path = pathlib.Path(os.path.expanduser("~/.git-credentials"))
    for line in path.read_text(encoding="utf-8").splitlines():
        if "github.com" in line:
            pw = urllib.parse.urlparse(line.strip()).password
            if pw:
                return urllib.parse.unquote(pw)
    raise SystemExit("no github token in ~/.git-credentials")


TOKEN = load_token()


def api(method: str, path: str, payload=None):
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(API + path, data=data, method=method)
    req.add_header("Authorization", f"Bearer {TOKEN}")
    req.add_header("Accept", "application/vnd.github+json")
    req.add_header("Content-Type", "application/json")
    req.add_header("User-Agent", "opencode-agent")
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            body = r.read().decode()
            return r.status, (json.loads(body) if body else {})
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read().decode() or "{}")


def parse(md_path: pathlib.Path) -> dict:
    # utf-8-sig: chịu được BOM (Set-Content của PowerShell hay thêm vào)
    text = md_path.read_text(encoding="utf-8-sig")
    m = FM_RE.match(text)
    meta: dict[str, object] = {}
    body = text
    if m:
        body = text[m.end():]
        for line in m.group(1).splitlines():
            if ":" in line and not line.startswith(" "):
                k, v = line.split(":", 1)
                meta[k.strip()] = v.strip()
    return {
        "id": str(meta.get("id", md_path.name[:2])),
        "severity": str(meta.get("severity", "p3")).lower(),
        "title": str(meta.get("title", md_path.stem)),
        "body": body.strip(),
        "file": md_path.name,
    }


def absolutise_links(body: str) -> str:
    def repl(m: re.Match) -> str:
        target = m.group(1)
        url = f"https://github.com/{REPO}/blob/{BRANCH}/issue/{target}"
        return f"]({url})"

    return LINK_RE.sub(repl, body)


def ensure_labels() -> None:
    for name, (color, desc) in LABELS.items():
        status, res = api("POST", f"/repos/{REPO}/labels", {"name": name, "color": color, "description": desc})
        print(f"label {name}: {status}" + ("" if status == 201 else f" ({res.get('message')})"))


def existing_titles() -> set[str]:
    titles: set[str] = set()
    page = 1
    while True:
        status, res = api("GET", f"/repos/{REPO}/issues?state=all&per_page=100&page={page}")
        if status != 200 or not res:
            break
        titles.update(i["title"] for i in res if "pull_request" not in i)
        if len(res) < 100:
            break
        page += 1
    return titles


def main() -> None:
    ensure_labels()
    have = existing_titles()
    files = sorted(ISSUE_DIR.glob("[0-9][0-9]-*.md"))
    created, skipped = [], []

    for path in files:
        item = parse(path)
        title = f"[{item['id']}] {item['title']}"
        if title in have:
            skipped.append(title)
            continue
        body = absolutise_links(item["body"])
        header = (
            f"> Nguồn: [`issue/{item['file']}`](https://github.com/{REPO}/blob/{BRANCH}/issue/{item['file']}) "
            f"· log đầy đủ: [`issue/README.md`](https://github.com/{REPO}/blob/{BRANCH}/issue/README.md)\n"
            f"> Severity **{item['severity'].upper()}**.\n\n"
        )
        status, res = api(
            "POST",
            f"/repos/{REPO}/issues",
            {"title": title, "body": header + body, "labels": [item["severity"]]},
        )
        if status == 201:
            created.append((res["number"], title, res["html_url"]))
            print(f"#{res['number']}  {title}")
        else:
            print(f"FAILED {title}: {status} {res.get('message')}")

    print(f"\ncreated={len(created)} skipped={len(skipped)}")
    for n, title, url in created:
        print(f"#{n:<3} {url}")


if __name__ == "__main__":
    main()
