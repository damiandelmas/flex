"""Retrieve: query execution. Matrix multiply, named presets, direct SQL.

Standalone usage (no MCP required):
    from flex.retrieve.execute import open_cell_for_query, execute

    db = open_cell_for_query('my_cell')
    rows = execute(db, "SELECT v.id FROM vec_ops('similar:query') v LIMIT 10")
    db.close()
"""
