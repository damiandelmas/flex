# Flex Markdown / Obsidian Module

This module indexes a folder of Markdown files as a Flex cell. If the folder
contains `.obsidian/`, the cell type is recorded as `obsidian`; otherwise it is
recorded as `markdown`. Both paths use the same compile pipeline and query
surface.

## Setup

For an Obsidian vault:

```bash
flex init --module obsidian --vault /path/to/vault --name my_vault
```

For a plain Markdown folder:

```bash
flex index /path/to/notes --name my_notes
```

After setup, orient the cell:

```bash
flex core search --cell my_vault "@orient"
```

The init path also registers the cell with `lifecycle='watch'` and the vault
root as `watch_path`, so the worker can keep the cell current.

## What Gets Indexed

- Every `.md` file becomes one source.
- Heading sections become searchable chunks.
- Frontmatter tags and inline `#tags` become source metadata.
- Frontmatter aliases are stored for lookup and wikilink resolution.
- Obsidian wikilinks become resolved link edges when targets exist.
- Unresolved wikilinks become ghost-note rows.
- Dataview inline fields such as `status:: active` become queryable rows.
- Heading hierarchy is stored in `_edges_tree`.

Source files are never modified. Template syntax, tags, and wikilink display
forms are cleaned only for embedding input.

## Query Surface

Start with:

```bash
flex core search --cell my_vault "@orient"
```

Primary views:

- `notes`: one row per file with folder, tags, aliases, link counts, and graph columns.
- `sections`: one row per chunk with content, heading metadata, offsets, folder, and tags.
- `chunks` and `sources`: generic Flex views generated from the cell schema.

Useful presets:

- `@hubs`: structurally important notes.
- `@orphans`: notes with no incoming or outgoing wikilinks.
- `@ghost-notes`: unresolved wikilink targets.
- `@communities`: note groups from graph enrichment when available.
- `@orient`: Markdown/Obsidian cell guide with mounted instructions, views,
  presets, counts, and sample sections.

Example:

```sql
SELECT v.score, s.note_title, s.section_title, s.content
FROM vec_ops('similar:project planning',
             'SELECT id FROM sections WHERE tags LIKE ''%project%''') v
JOIN sections s ON v.id = s.id
ORDER BY v.score DESC
LIMIT 10;
```

## Local Notes

You can add local instruction notes without editing the package:

```text
~/.flex/instructions/markdown.md
~/.flex/instructions/obsidian.md
```

Those files appear in the `cell_docs` block of `@orient` along with this
module's packaged instructions.
