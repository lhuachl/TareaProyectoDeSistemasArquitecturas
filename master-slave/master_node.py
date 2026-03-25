"""
master_node.py — Nodo maestro (Primary)
========================================
Responsabilidad ÚNICA: escribir datos.
  - add_task    → crea una tarea nueva
  - complete_task → alterna el estado completado
  - delete_task → elimina una tarea

Cada operación de escritura dispara una replicación a todos los slaves
registrados mediante register_slave().

Funciones Rust (PyO3) usadas aquí:
  · generate_id(title, timestamp_ms)  → ID hex de 16 chars, más rápido que uuid4
  · validate_title(title)             → validación de longitud/vacío, O(n) en Rust
"""

from __future__ import annotations

import threading
import time
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from slave_node import SlaveNode

try:
    import modulo_rust
    _RUST = True
except ImportError:
    _RUST = False
    print("⚠️  modulo_rust no disponible — compila con: maturin develop --release")


class MasterNode:
    """Nodo primario: acepta todas las escrituras y replica a los slaves."""

    def __init__(self) -> None:
        self._tasks: dict[str, dict] = {}
        self._slaves: list[SlaveNode] = []
        self._lock = threading.Lock()

    # ------------------------------------------------------------------ #
    #  Registro de slaves                                                  #
    # ------------------------------------------------------------------ #

    def register_slave(self, slave: SlaveNode) -> None:
        """Conecta un slave para recibir replicación tras cada escritura."""
        self._slaves.append(slave)

    # ------------------------------------------------------------------ #
    #  Escrituras (solo el master puede llamar a estos métodos)           #
    # ------------------------------------------------------------------ #

    def add_task(self, title: str) -> dict:
        """Crea una tarea, la almacena y replica a los slaves."""

        # --- validación en Rust (rápido) ---
        if _RUST:
            if not modulo_rust.validate_title(title):
                raise ValueError(f"Título inválido: '{title}'")
        else:
            if not title.strip() or len(title) > 200:
                raise ValueError(f"Título inválido: '{title}'")

        ts_ms = int(time.time() * 1_000)

        # --- generación de ID en Rust (más rápido que uuid4) ---
        if _RUST:
            task_id = modulo_rust.generate_id(title, ts_ms)
        else:
            import hashlib
            task_id = hashlib.md5(f"{title}{ts_ms}".encode()).hexdigest()[:16]

        task: dict = {
            "id": task_id,
            "title": title.strip(),
            "completed": False,
            "created_at": ts_ms // 1_000,
        }

        with self._lock:
            self._tasks[task_id] = task
            self._replicate()

        return task

    def complete_task(self, task_id: str) -> dict | None:
        """Alterna el estado completed de una tarea y replica."""
        with self._lock:
            if task_id not in self._tasks:
                return None
            self._tasks[task_id]["completed"] = not self._tasks[task_id]["completed"]
            self._replicate()
            return dict(self._tasks[task_id])

    def delete_task(self, task_id: str) -> bool:
        """Elimina una tarea y replica."""
        with self._lock:
            if task_id not in self._tasks:
                return False
            del self._tasks[task_id]
            self._replicate()
            return True

    # ------------------------------------------------------------------ #
    #  Replicación interna                                                 #
    # ------------------------------------------------------------------ #

    def _replicate(self) -> None:
        """Envía snapshot actual a todos los slaves (llamado dentro del lock)."""
        snapshot = list(self._tasks.values())
        for slave in self._slaves:
            slave.sync(snapshot)

    # ------------------------------------------------------------------ #
    #  Utilidades                                                          #
    # ------------------------------------------------------------------ #

    @property
    def task_count(self) -> int:
        return len(self._tasks)

    def status(self) -> dict:
        return {
            "node": "master",
            "role": "primary (write)",
            "tasks": self.task_count,
            "slaves_connected": len(self._slaves),
            "rust_enabled": _RUST,
        }
