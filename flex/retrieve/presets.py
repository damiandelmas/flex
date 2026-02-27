"""
Flex Presets — SQL skills stored in the cell.

Presets live in the _presets table (name, description, sql).
The sql column contains annotated SQL text with @multi, @query, @params.
Parse logic extracts structure from annotations — same format as .sql files.

Discovery:
    SELECT name, description FROM _presets;

Execution:
    PresetLoader(db).execute(db, 'orient')
    PresetLoader(db).execute(db, 'sessions', {'limit': 5})
"""

import re
import sqlite3
from pathlib import Path
from typing import Optional


class PresetLoader:
    """Load and execute SQL presets from the _presets table."""

    def __init__(self, db: sqlite3.Connection):
        self.db = db
        self._cache: dict[str, dict] = {}

    def list_presets(self) -> list[str]:
        """List available preset names."""
        try:
            rows = self.db.execute("SELECT name FROM _presets ORDER BY name").fetchall()
            return [r[0] for r in rows]
        except sqlite3.OperationalError:
            return []

    def load(self, name: str) -> dict:
        """
        Load a preset from the _presets table.

        Returns:
            {
                'name': str,
                'description': str,
                'multi': bool,
                'queries': [{'name': str, 'sql': str}, ...],
                'defaults': {}
            }
        """
        if name in self._cache:
            return self._cache[name]

        row = self.db.execute(
            "SELECT sql FROM _presets WHERE name = ?", (name,)
        ).fetchone()
        if row is None:
            raise KeyError(f"Preset not found: {name}")

        text = row[0]
        preset = self._parse(text, name)
        self._cache[name] = preset
        return preset

    def execute(self, db: sqlite3.Connection, name: str,
                params: dict = None) -> list[dict]:
        """
        Execute a preset and return results.

        For @multi presets, returns list of {query_name: results}.
        For single presets, returns list of row dicts.
        """
        preset = self.load(name)
        params = {**preset.get('defaults', {}), **(params or {})}

        # Validate required params before execution
        missing = self._check_missing_params(preset, params)
        if missing:
            return [{"error": f"Missing required parameter(s): {', '.join(missing)}",
                      "usage": f"@{name} " + " ".join(f"{p}=VALUE" for p in missing)}]

        if preset['multi']:
            results = []
            for query in preset['queries']:
                sql = self._interpolate(query['sql'], params)
                sql = self._materialize_vec_ops(db, sql)
                if sql.startswith('{"error"'):
                    results.append({
                        'query': query['name'],
                        'error': sql
                    })
                    continue
                try:
                    rows = db.execute(sql).fetchall()
                    results.append({
                        'query': query['name'],
                        'results': [dict(r) for r in rows]
                    })
                except sqlite3.OperationalError as e:
                    results.append({
                        'query': query['name'],
                        'error': str(e)
                    })
            return results
        else:
            sql = self._interpolate(preset['queries'][0]['sql'], params)
            sql = self._materialize_vec_ops(db, sql)
            if sql.startswith('{"error"'):
                return [{"error": sql}]
            rows = db.execute(sql).fetchall()
            return [dict(r) for r in rows]

    @staticmethod
    def _check_missing_params(preset: dict, params: dict) -> list[str]:
        """Check if required params are missing. Returns list of missing param names."""
        param_str = preset.get('params', '')
        if not param_str:
            return []
        missing = []
        for part in param_str.split(','):
            part = part.strip()
            if '(default' in part:
                continue  # has default — not required
            name = part.split()[0]  # "session (required)" → "session"
            if name and name != '(required)' and name not in params:
                missing.append(name)
        return missing

    @staticmethod
    def _materialize_vec_ops(db: sqlite3.Connection, sql: str) -> str:
        """Materialize vec_ops() calls in SQL if present."""
        if 'vec_ops(' not in sql:
            return sql
        try:
            from flex.retrieve.vec_ops import materialize_vec_ops
            return materialize_vec_ops(db, sql)
        except ImportError:
            return sql

    @staticmethod
    def _parse(text: str, name: str) -> dict:
        """Parse annotated SQL text into structured dict."""
        preset = {
            'name': name,
            'description': '',
            'params': '',
            'multi': False,
            'queries': [],
            'defaults': {}
        }

        current_query_name = None
        current_sql_lines = []

        for line in text.split('\n'):
            stripped = line.strip()

            # Parse annotations
            if stripped.startswith('-- @'):
                match = re.match(r'-- @(\w+):\s*(.+)', stripped)
                if match:
                    key, value = match.group(1), match.group(2).strip()
                    if key == 'name':
                        preset['name'] = value
                    elif key == 'description':
                        preset['description'] = value
                    elif key == 'multi':
                        preset['multi'] = value.lower() == 'true'
                    elif key == 'query':
                        # Save previous query
                        if current_query_name is not None and current_sql_lines:
                            sql = '\n'.join(current_sql_lines).strip()
                            if sql:
                                preset['queries'].append({
                                    'name': current_query_name,
                                    'sql': sql
                                })
                        current_query_name = value
                        current_sql_lines = []
                    elif key in ('param', 'params'):
                        preset['params'] = value
                        # Parse "name (default: value)" patterns
                        for part in value.split(','):
                            part = part.strip()
                            default_match = re.match(
                                r'(\w+)\s*\(default:\s*(.+?)\)', part)
                            if default_match:
                                pname = default_match.group(1)
                                pval = default_match.group(2).strip()
                                try:
                                    preset['defaults'][pname] = int(pval)
                                except ValueError:
                                    preset['defaults'][pname] = pval
                continue

            # Skip empty comment lines
            if stripped == '--':
                continue

            current_sql_lines.append(line)

        # Save last query.
        # If no -- @query: was ever seen, this is a single-query preset — use 'default'.
        final_name = current_query_name if current_query_name is not None else 'default'
        if current_sql_lines:
            sql = '\n'.join(current_sql_lines).strip()
            if sql:
                preset['queries'].append({
                    'name': final_name,
                    'sql': sql
                })

        return preset

    @staticmethod
    def _interpolate(sql: str, params: dict) -> str:
        """Replace :named_params with values (escaped)."""
        for key, value in params.items():
            placeholder = f":{key}"
            if placeholder in sql:
                if isinstance(value, str):
                    escaped = value.replace("'", "''")
                    sql = sql.replace(placeholder, f"'{escaped}'")
                else:
                    sql = sql.replace(placeholder, str(value))
        return sql


def install_presets(db: sqlite3.Connection, preset_dir: Path):
    """Read .sql files from a directory and INSERT into _presets table.

    Used by init scripts to bake filesystem presets into the cell.
    The .sql files are the authoring format; the DB is the runtime source.
    """
    preset_dir = Path(preset_dir)
    if not preset_dir.exists():
        return

    installed = []
    for f in sorted(preset_dir.glob('*.sql')):
        text = f.read_text()
        # Extract name, description, and params from annotations
        parsed = PresetLoader._parse(text, f.stem)
        db.execute(
            "INSERT OR REPLACE INTO _presets (name, description, params, sql) VALUES (?, ?, ?, ?)",
            (parsed['name'], parsed['description'], parsed.get('params', ''), text)
        )
        installed.append(parsed['name'])
    db.commit()

    if installed:
        from flex.core import log_op
        log_op(db, 'install_presets', '_presets',
               params={'presets': installed, 'source_dir': str(preset_dir)},
               rows_affected=len(installed), source='presets.py')
