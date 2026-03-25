use pyo3::prelude::*;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// Modelo interno de tarea — solo para (de)serializar JSON en las funciones Rust.
#[derive(serde::Serialize, serde::Deserialize)]
struct Task {
    id: String,
    title: String,
    completed: bool,
    created_at: u64,
}

#[pymodule]
mod modulo_rust {
    use super::{DefaultHasher, Hash, Hasher, Task};
    use pyo3::prelude::*;

    // ------------------------------------------------------------------ //
    //  generate_id                                                         //
    //  Genera un ID hexadecimal de 16 chars a partir de título + timestamp //
    //  Más rápido que uuid4 de Python para alta concurrencia.             //
    // ------------------------------------------------------------------ //
    #[pyfunction]
    fn generate_id(title: &str, timestamp_ms: u64) -> String {
        let mut h = DefaultHasher::new();
        title.hash(&mut h);
        timestamp_ms.hash(&mut h);
        format!("{:016x}", h.finish())
    }

    // ------------------------------------------------------------------ //
    //  validate_title                                                      //
    //  Comprueba que el título no esté vacío y tenga ≤ 200 caracteres.    //
    // ------------------------------------------------------------------ //
    #[pyfunction]
    fn validate_title(title: &str) -> bool {
        let t = title.trim();
        !t.is_empty() && t.len() <= 200
    }

    // ------------------------------------------------------------------ //
    //  filter_tasks                                                        //
    //  Filtra una lista de tareas (JSON) por query en el título            //
    //  (case-insensitive). Ventaja real para listas > 1 000 tareas.       //
    // ------------------------------------------------------------------ //
    #[pyfunction]
    fn filter_tasks(tasks_json: &str, query: &str) -> PyResult<String> {
        let tasks: Vec<Task> = serde_json::from_str(tasks_json)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;

        let q = query.to_lowercase();
        let filtered: Vec<&Task> = tasks
            .iter()
            .filter(|t| t.title.to_lowercase().contains(&q))
            .collect();

        serde_json::to_string(&filtered)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))
    }

    // ------------------------------------------------------------------ //
    //  count_completed                                                     //
    //  Cuenta cuántas tareas tienen completed = true.                     //
    //  Más rápido que sum() en Python para listas grandes.                //
    // ------------------------------------------------------------------ //
    #[pyfunction]
    fn count_completed(tasks_json: &str) -> PyResult<usize> {
        let tasks: Vec<Task> = serde_json::from_str(tasks_json)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
        Ok(tasks.iter().filter(|t| t.completed).count())
    }
}
