"""
Flexsearch Presets — named SQL queries.

Read .sql files with annotations, interpolate :named_params, execute.

Annotations:
    -- @name: introspect
    -- @description: Full cell orientation
    -- @multi: true
    -- @query: shape
    SELECT COUNT(*) FROM _raw_chunks;
"""

import re
import sqlite3
from pathlib import Path
from typing import Optional


class PresetLoader:
    """Load and execute SQL preset files."""

    def __init__(self, preset_dir: Path):
        self.preset_dir = preset_dir
        self._cache: dict[str, dict] = {}

    def list_presets(self) -> list[str]:
        """List available preset names."""
        if not self.preset_dir.exists():
            return []
        return [f.stem for f in self.preset_dir.glob('*.sql')]

    def load(self, name: str) -> dict:
        """
        Parse a preset .sql file.

        Returns:
            {
                'name': str,
                'description': str,
                'multi': bool,
                'queries': [{'name': str, 'sql': str}, ...]
            }
        """
        if name in self._cache:
            return self._cache[name]

        path = self.preset_dir / f"{name}.sql"
        if not path.exists():
            raise FileNotFoundError(f"Preset not found: {name}")

        text = path.read_text()
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
        params = params or {}

        if preset['multi']:
            results = []
            for query in preset['queries']:
                sql = self._interpolate(query['sql'], params)
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
            rows = db.execute(sql).fetchall()
            return [dict(r) for r in rows]

    @staticmethod
    def _parse(text: str, name: str) -> dict:
        """Parse a .sql preset file into structured dict."""
        preset = {
            'name': name,
            'description': '',
            'multi': False,
            'queries': []
        }

        current_query_name = 'default'
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
                        if current_sql_lines:
                            sql = '\n'.join(current_sql_lines).strip()
                            if sql:
                                preset['queries'].append({
                                    'name': current_query_name,
                                    'sql': sql
                                })
                        current_query_name = value
                        current_sql_lines = []
                    elif key == 'param':
                        pass  # Future: parameter validation
                continue

            # Skip empty comment lines
            if stripped == '--':
                continue

            current_sql_lines.append(line)

        # Save last query
        if current_sql_lines:
            sql = '\n'.join(current_sql_lines).strip()
            if sql:
                preset['queries'].append({
                    'name': current_query_name,
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
