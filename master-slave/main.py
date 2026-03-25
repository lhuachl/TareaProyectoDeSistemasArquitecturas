"""
main.py — FastAPI + Master-Slave Todo
========================================
Rutas:
  GET  /                         → página principal
  GET  /slave/{id}/tasks         → lista de tareas de un slave (partial HTMX)
  POST /tasks                    → crear tarea (escribe en master)
  DELETE /tasks/{id}             → eliminar tarea (escribe en master)
  PUT  /tasks/{id}/toggle        → marcar completada (escribe en master)
  GET  /status                   → estado JSON de nodos

Ejecutar:
  # 1. Compilar el módulo Rust (solo cuando cambies src/lib.rs)
  maturin develop

  # 2. Levantar el servidor (NO uses uv run — sobreescribe el .so compilado)
  .venv/bin/uvicorn main:app --host 0.0.0.0 --port 3000 --reload
"""

from contextlib import asynccontextmanager

from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
import uvicorn

from master_node import MasterNode
from slave_node import SlaveNode

# ------------------------------------------------------------------ #
#  Nodos                                                               #
# ------------------------------------------------------------------ #

master = MasterNode()
slave1 = SlaveNode(node_id=1, replication_lag=0.0)   # réplica rápida
slave2 = SlaveNode(node_id=2, replication_lag=0.8)   # 800 ms lag demo

master.register_slave(slave1)
master.register_slave(slave2)


# ------------------------------------------------------------------ #
#  App                                                                 #
# ------------------------------------------------------------------ #

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Seed inicial para que el demo tenga datos desde el arranque
    for title in [
        "Estudiar arquitectura master-slave",
        "Implementar funciones Rust con PyO3",
        "Demostrar replication lag en el browser",
    ]:
        master.add_task(title)
    yield


app = FastAPI(title="Master-Slave Todo", lifespan=lifespan)
templates = Jinja2Templates(directory="templates")


def _get_slave(slave_id: int) -> SlaveNode:
    return slave1 if slave_id == 1 else slave2


# ------------------------------------------------------------------ #
#  Rutas de lectura (slaves)                                           #
# ------------------------------------------------------------------ #

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse(request, "index.html")


@app.get("/slave/{slave_id}/tasks", response_class=HTMLResponse)
async def slave_tasks(request: Request, slave_id: int, q: str = ""):
    slave = _get_slave(slave_id)
    tasks = slave.get_tasks(query=q)
    stats = slave.get_stats()
    return templates.TemplateResponse(
        request,
        "partials/task_list.html",
        {"tasks": tasks, "stats": stats, "slave_id": slave_id, "query": q},
    )


@app.get("/status")
async def status():
    return {
        "master": master.status(),
        "slave1": slave1.get_stats(),
        "slave2": slave2.get_stats(),
    }


# ------------------------------------------------------------------ #
#  Rutas de escritura (master)                                         #
# ------------------------------------------------------------------ #

@app.post("/tasks", response_class=HTMLResponse)
async def create_task(request: Request, title: str = Form(...)):
    try:
        master.add_task(title)
    except ValueError:
        pass  # título inválido: silencioso en el demo

    tasks = slave1.get_tasks()
    stats = slave1.get_stats()
    return templates.TemplateResponse(
        request,
        "partials/task_list.html",
        {"tasks": tasks, "stats": stats, "slave_id": 1, "query": ""},
    )


@app.delete("/tasks/{task_id}", response_class=HTMLResponse)
async def delete_task(request: Request, task_id: str):
    master.delete_task(task_id)
    tasks = slave1.get_tasks()
    stats = slave1.get_stats()
    return templates.TemplateResponse(
        request,
        "partials/task_list.html",
        {"tasks": tasks, "stats": stats, "slave_id": 1, "query": ""},
    )


@app.put("/tasks/{task_id}/toggle", response_class=HTMLResponse)
async def toggle_task(request: Request, task_id: str):
    master.complete_task(task_id)
    tasks = slave1.get_tasks()
    stats = slave1.get_stats()
    return templates.TemplateResponse(
        request,
        "partials/task_list.html",
        {"tasks": tasks, "stats": stats, "slave_id": 1, "query": ""},
    )


# ------------------------------------------------------------------ #
#  Entrypoint                                                          #
# ------------------------------------------------------------------ #

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=3000, reload=True)