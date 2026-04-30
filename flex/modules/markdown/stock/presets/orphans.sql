-- @name: orphans
-- @description: Notes with no wikilinks (outgoing or incoming)

SELECT title, folder, note_created, file_modified
FROM notes
WHERE outgoing_links = 0 AND backlinks = 0
ORDER BY file_modified DESC
