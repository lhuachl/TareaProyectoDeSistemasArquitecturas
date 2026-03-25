"""
slave_node.py — Nodo esclavo (Replica)
========================================
Responsabilidad ÚNICA: leer datos.
  - get_tasks(query)  → devuelve lista (filtrada si hay query)
  - get_stats()       → resumen del nodo

El master llama a sync() después de cada escritura.
El replication_lag simula la latencia de red entre master y replica.

Funciones Rust (PyO3) usadas aquí:
  · filter_tasks(tasks_json, query)  → filtrado case-insensitive, O(n) en Rust
  · count_completed(tasks_json)      → conteo de completadas, O(n) en Rust
"""

from __future__ import annotations

import json
import threading
import time

try:
    import modulo_rust
    _RUST = True
except ImportError:
    _RUST = False


class SlaveNode:
    """
    Nodo réplica: solo lectura.
    Recibe datos del master con un lag configurable (simula latencia real).
    """

    def __init__(self, node_id: int, replication_lag: float = 0.0) -> None:
        self.node_id = node_id
        self.replication_lag = replication_lag  # segundos
        self._tasks: list[dict] = []
        self._lock = threading.Lock()
        self.last_sync: float = 0.0
        self.sync_count: int = 0

    # ------------------------------------------------------------------ #
    #  Replicación (llamado por el master)                                 #
    # ------------------------------------------------------------------ #

    def sync(self, tasks: list[dict]) -> None:
        """
        Recibe un snapshot del master.
        - lag = 0  → sincronización inmediata (sin thread extra)
        - lag > 0  → actualización diferida en background thread
        """
        if self.replication_lag == 0.0:
            # Síncrono: slave1 siempre consistente con el master
            with self._lock:
                self._tasks = [dict(t) for t in tasks]
                self.last_sync = time.time()
                self.sync_count += 1
        else:
            # Asíncrono con lag simulado (demo de replication lag)
            snapshot = [dict(t) for t in tasks]

            def _apply() -> None:
                time.sleep(self.replication_lag)
                with self._lock:
                    self._tasks = snapshot
                    self.last_sync = time.time()
                    self.sync_count += 1

            threading.Thread(target=_apply, daemon=True).start()

    # ------------------------------------------------------------------ #
    #  Lecturas                                                            #
    # ------------------------------------------------------------------ #

    def get_tasks(self, query: str = "") -> list[dict]:
        """
        Devuelve la lista de tareas.
        Si hay query, filtra en Rust (case-insensitive) — más rápido para listas grandes.
        """
        with self._lock:
            snapshot = list(self._tasks)

        if not query.strip():
            return snapshot

        # --- filtrado en Rust ---
        if _RUST:
            filtered_json = modulo_rust.filter_tasks(json.dumps(snapshot), query)
            return json.loads(filtered_json)
        else:
            q = query.lower()
            return [t for t in snapshot if q in t["title"].lower()]

    def get_stats(self) -> dict:
        """Estadísticas del nodo — conteo completadas en Rust."""
        with self._lock:
            snapshot = list(self._tasks)

        total = len(snapshot)

        # --- conteo en Rust ---
        if _RUST and snapshot:
            completed = modulo_rust.count_completed(json.dumps(snapshot))
        else:
            completed = sum(1 for t in snapshot if t["completed"])

        last_sync_str = (
            time.strftime("%H:%M:%S", time.localtime(self.last_sync))
            if self.last_sync
            else "—"
        )

        return {
            "node_id": self.node_id,
            "total": total,
            "completed": completed,
            "pending": total - completed,
            "replication_lag_ms": int(self.replication_lag * 1_000),
            "last_sync": last_sync_str,
            "sync_count": self.sync_count,
            "rust_enabled": _RUST,
        }
