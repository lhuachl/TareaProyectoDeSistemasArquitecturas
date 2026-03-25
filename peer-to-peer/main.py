"""
P2P To-Do List — FastAPI node
Environment variables:
  PORT  → port this node listens on (default 8000)
  PEERS → comma-separated URLs of peer nodes, e.g. "http://node_b:8002,http://node_c:8003"
  NODE_ID → human-readable name, e.g. "Node A"
"""

import asyncio
import logging
import os
from typing import Any, Optional

import httpx
from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel

# ── Config ────────────────────────────────────────────────────────────────────
PORT = int(os.getenv("PORT", "8000"))
NODE_ID = os.getenv("NODE_ID", f"Node-{PORT}")
RAW_PEERS = os.getenv("PEERS", "")
PEERS: list[str] = [p.strip() for p in RAW_PEERS.split(",") if p.strip()]

# ── State (per-node, not shared) ──────────────────────────────────────────────
tasks: list[dict] = []
consensus_log: list[dict] = []

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format=f"[%(asctime)s] [{NODE_ID}] %(levelname)s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── App ───────────────────────────────────────────────────────────────────────
app = FastAPI(title=f"P2P Node — {NODE_ID}")
templates = Jinja2Templates(directory="templates")


# ── Schemas ───────────────────────────────────────────────────────────────────
class TaskProposal(BaseModel):
    text: str


class VoteRequest(BaseModel):
    task_id: str
    text: str


class CommitRequest(BaseModel):
    task_id: str
    text: str


# ── Local validation ──────────────────────────────────────────────────────────
def validate_task(text: str) -> tuple[bool, str]:
    """Each node independently validates the task."""
    text = text.strip()
    if not text:
        return False, "empty text"
    if len(text) > 200:
        return False, "text too long (max 200 chars)"
    if any(t["text"].lower() == text.lower() for t in tasks):
        return False, "duplicate task"
    return True, "ok"


def _add_log(entry: dict) -> None:
    consensus_log.append(entry)
    if len(consensus_log) > 100:
        consensus_log.pop(0)


# ── Routes ────────────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse(
        request=request,
        name="index.html",
        context={"node_id": NODE_ID, "port": PORT, "peers": PEERS},
    )


@app.get("/tasks")
async def get_tasks():
    """Return this node's task list as JSON."""
    return {"node_id": NODE_ID, "tasks": tasks}


@app.get("/log")
async def get_log():
    """Return the consensus activity log as JSON."""
    return {"node_id": NODE_ID, "log": list(reversed(consensus_log))}


# ── HTMX partial fragments ─────────────────────────────────────────────────────

@app.get("/stats/partial", response_class=HTMLResponse)
async def stats_partial():
    """HTMX fragment: renders the stats cards only."""
    total   = len(tasks)
    done    = sum(1 for t in tasks if t["done"])
    pending = total - done
    return HTMLResponse(f"""
    <div class="stats-row">
        <div class="stat-card">
            <span class="stat-num">{total}</span>
            <span class="stat-label">TOTAL</span>
        </div>
        <div class="stat-card accent">
            <span class="stat-num">{done}</span>
            <span class="stat-label">DONE</span>
        </div>
        <div class="stat-card">
            <span class="stat-num">{pending}</span>
            <span class="stat-label">PENDING</span>
        </div>
    </div>
    """)


@app.get("/tasks/partial", response_class=HTMLResponse)
async def tasks_partial():
    """HTMX fragment: renders the task list only."""
    if not tasks:
        return HTMLResponse("""
        <ul class="task-list">
          <li class="empty-state">
            <svg width="32" height="32" viewBox="0 0 24 24" fill="none" style="opacity:.3;margin-bottom:10px">
              <rect x="3" y="3" width="18" height="18" rx="2" stroke="#00f5c8" stroke-width="1.5"/>
              <line x1="8" y1="12" x2="16" y2="12" stroke="#00f5c8" stroke-width="1.5"/>
              <line x1="12" y1="8" x2="12" y2="16" stroke="#00f5c8" stroke-width="1.5"/>
            </svg>
            <p>MATRIX VAC\u00cdA</p>
            <p class="empty-sub">Prop\u00f3n tu primera directiva</p>
          </li>
        </ul>""")

    items = ""
    for t in reversed(tasks):
        done_cls = " done" if t["done"] else ""
        items += f"""
        <li class="task-item{done_cls}">
            <div class="check-box">
                <svg class="check-icon" viewBox="0 0 12 12">
                    <polyline points="2,6 5,9 10,3"/>
                </svg>
            </div>
            <span class="task-text">{_esc(t['text'])}</span>
            <span class="task-id-badge">#{t['id']}</span>
            <button class="action-btn toggle-btn"
                    hx-patch="/tasks/{t['id']}/toggle"
                    hx-target="#task-list-wrap"
                    hx-swap="innerHTML"
                    title="Toggle">
                <svg width="14" height="14" viewBox="0 0 24 24" fill="none">
                    <polyline points="20,6 9,17 4,12" stroke="currentColor" stroke-width="2"
                              stroke-linecap="round" stroke-linejoin="round"/>
                </svg>
            </button>
            <button class="action-btn delete-btn"
                    hx-delete="/tasks/{t['id']}"
                    hx-target="#task-list-wrap"
                    hx-swap="innerHTML"
                    hx-confirm="\u00bfEliminar esta tarea?"
                    title="Eliminar">
                <svg width="14" height="14" viewBox="0 0 24 24" fill="none">
                    <line x1="18" y1="6" x2="6" y2="18" stroke="currentColor"
                          stroke-width="2" stroke-linecap="round"/>
                    <line x1="6" y1="6" x2="18" y2="18" stroke="currentColor"
                          stroke-width="2" stroke-linecap="round"/>
                </svg>
            </button>
        </li>"""

    return HTMLResponse(f'<ul class="task-list">{items}</ul>')


@app.get("/log/partial", response_class=HTMLResponse)
async def log_partial():
    """HTMX fragment: renders the last 20 consensus log entries."""
    recent = list(reversed(consensus_log))[:20]
    if not recent:
        return HTMLResponse('<p class="log-empty">SIN ACTIVIDAD AÚN</p>')

    html = ""
    for entry in recent:
        ev   = entry.get("event", "?")
        node = entry.get("node", "?")
        tid  = entry.get("task_id", "")
        text = entry.get("text", "")

        if ev == "PROPOSE":
            badge = '<span class="ev ev-propose">PROPOSE</span>'
            detail = f'<span class="log-node">{_esc(node)}</span> propone: <em>{_esc(text)}</em>'
        elif ev == "VOTE":
            vote_val = entry.get("vote", False)
            reason   = entry.get("reason", "")
            badge    = '<span class="ev ev-vote">VOTE</span>'
            color    = "vote-yes" if vote_val else "vote-no"
            detail   = (
                f'<span class="log-node">{_esc(node)}</span> '
                f'→ <span class="{color}">{"✔ sí" if vote_val else "✘ no"}</span>'
                f' <span class="log-meta">({_esc(reason)})</span>'
            )
        elif ev == "CONSENSUS":
            approved = entry.get("approved", False)
            pos      = entry.get("positive", 0)
            tot      = entry.get("total", 0)
            if approved:
                badge  = '<span class="ev ev-consensus-ok">APPROVED</span>'
                detail = f'<span class="log-node">{_esc(node)}</span> {pos}/{tot} votos ✔'
            else:
                badge  = '<span class="ev ev-consensus-no">REJECTED</span>'
                detail = f'<span class="log-node">{_esc(node)}</span> {pos}/{tot} votos ✘'
        elif ev == "COMMIT":
            badge  = '<span class="ev ev-commit">COMMIT</span>'
            detail = f'<span class="log-node">{_esc(node)}</span> guardó #{tid}'
        else:
            badge  = f'<span class="ev">{_esc(ev)}</span>'
            detail = f'<span class="log-node">{_esc(node)}</span>'

        html += f'<div class="log-entry">{badge} {detail}</div>\n'

    return HTMLResponse(html)


def _esc(s: str) -> str:
    """Minimal HTML escaping."""
    return (
        str(s)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


@app.post("/propose")
async def propose(
    request: Request,
    text: Optional[str] = Form(default=None),
):
    """
    STEP 1 — Entry point: user submits a task here.
    Accepts both application/x-www-form-urlencoded (HTMX) and JSON bodies.
    This node validates locally, then asks peers to vote.
    """
    import uuid

    # Support both form and JSON bodies
    if text is None:
        body = await request.json()
        text = body.get("text", "")

    task_id = str(uuid.uuid4())[:8]
    text = text.strip()

    log.info("PROPOSE  id=%s  text=%r", task_id, text)
    _add_log({"event": "PROPOSE", "task_id": task_id, "text": text, "node": NODE_ID})

    # ── Step 2: local validation ───────────────────────────────────────────
    local_valid, reason = validate_task(text)
    votes: list[dict[str, Any]] = [
        {"node": NODE_ID, "vote": local_valid, "reason": reason}
    ]
    log.info("VOTE_LOCAL  id=%s  vote=%s  reason=%s", task_id, local_valid, reason)

    # ── Step 3: ask peers to vote ──────────────────────────────────────────
    vote_payload = {"task_id": task_id, "text": text}
    async with httpx.AsyncClient(timeout=3.0) as client:
        peer_tasks = [
            _request_vote(client, peer, vote_payload) for peer in PEERS
        ]
        peer_votes = await asyncio.gather(*peer_tasks, return_exceptions=True)

    for pv in peer_votes:
        if isinstance(pv, Exception):
            log.warning("VOTE_PEER error: %s", pv)
            votes.append({"node": "unknown", "vote": False, "reason": str(pv)})
        else:
            votes.append(pv)
            log.info(
                "VOTE_PEER  node=%s  vote=%s  reason=%s",
                pv["node"], pv["vote"], pv["reason"],
            )

    # ── Step 4: count votes ────────────────────────────────────────────────
    positive = sum(1 for v in votes if v["vote"])
    total = len(votes)
    approved = positive >= 2  # majority of 3 = 2+

    log.info(
        "CONSENSUS id=%s  votes=%d/%d  approved=%s",
        task_id, positive, total, approved,
    )
    _add_log({
        "event": "CONSENSUS",
        "task_id": task_id,
        "text": text,
        "positive": positive,
        "total": total,
        "approved": approved,
        "votes": votes,
        "node": NODE_ID,
    })

    if not approved:
        return {
            "approved": False,
            "task_id": task_id,
            "positive": positive,
            "total": total,
            "votes": votes,
            "reason": "insufficient votes",
        }

    # ── Step 5: commit locally ─────────────────────────────────────────────
    task_entry = {"id": task_id, "text": text, "done": False}
    tasks.append(task_entry)
    log.info("COMMIT_LOCAL  id=%s", task_id)
    _add_log({"event": "COMMIT", "task_id": task_id, "text": text, "node": NODE_ID})

    # ── Step 6: tell peers to commit ───────────────────────────────────────
    commit_payload = {"task_id": task_id, "text": text}
    async with httpx.AsyncClient(timeout=3.0) as client:
        commit_tasks = [
            _send_commit(client, peer, commit_payload) for peer in PEERS
        ]
        await asyncio.gather(*commit_tasks, return_exceptions=True)

    return {
        "approved": True,
        "task_id": task_id,
        "text": text,
        "positive": positive,
        "total": total,
        "votes": votes,
    }


@app.post("/vote")
async def vote(req: VoteRequest):
    """
    Called by the proposing node to ask this node's opinion.
    Each node decides independently.
    """
    log.info("VOTE_REQUEST  id=%s  text=%r", req.task_id, req.text)
    valid, reason = validate_task(req.text)
    log.info("VOTE_RESPONSE  id=%s  vote=%s  reason=%s", req.task_id, valid, reason)
    _add_log({
        "event": "VOTE",
        "task_id": req.task_id,
        "text": req.text,
        "vote": valid,
        "reason": reason,
        "node": NODE_ID,
    })
    return {"node": NODE_ID, "vote": valid, "reason": reason}


@app.post("/commit")
async def commit(req: CommitRequest):
    """
    Called by the proposing node after consensus is reached.
    This node saves the task to its own local state.
    """
    # avoid double-commit if this node already has the task
    if any(t["id"] == req.task_id for t in tasks):
        log.info("COMMIT_SKIP  id=%s  (already exists)", req.task_id)
        return {"status": "already_exists", "node": NODE_ID}

    task_entry = {"id": req.task_id, "text": req.text, "done": False}
    tasks.append(task_entry)
    log.info("COMMIT  id=%s  text=%r", req.task_id, req.text)
    _add_log({
        "event": "COMMIT",
        "task_id": req.task_id,
        "text": req.text,
        "node": NODE_ID,
    })
    return {"status": "committed", "node": NODE_ID}


@app.patch("/tasks/{task_id}/toggle")
async def toggle_task(task_id: str):
    """Toggle done/pending on this node only (local UI action). Returns HTML partial."""
    for t in tasks:
        if t["id"] == task_id:
            t["done"] = not t["done"]
            break
    return await tasks_partial()


@app.delete("/tasks/{task_id}")
async def delete_task(task_id: str):
    """Delete a task on this node only. Returns HTML partial."""
    global tasks
    tasks = [t for t in tasks if t["id"] != task_id]
    return await tasks_partial()


# ── Helpers ───────────────────────────────────────────────────────────────────

async def _request_vote(
    client: httpx.AsyncClient, peer: str, payload: dict
) -> dict:
    r = await client.post(f"{peer}/vote", json=payload)
    r.raise_for_status()
    return r.json()


async def _send_commit(
    client: httpx.AsyncClient, peer: str, payload: dict
) -> dict:
    try:
        r = await client.post(f"{peer}/commit", json=payload)
        r.raise_for_status()
        log.info("COMMIT_PEER  peer=%s  id=%s", peer, payload["task_id"])
        return r.json()
    except Exception as exc:
        log.warning("COMMIT_PEER_ERROR  peer=%s  err=%s", peer, exc)
        return {"status": "error", "peer": peer}


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=PORT, reload=False)

