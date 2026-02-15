-- @name: research-trail
-- @description: Web research activity — WebFetch and WebSearch operations
-- @params: limit (default: 50)

SELECT
    substr(source_id, 1, 8) as session,
    action,
    substr(content, 1, 300) as content,
    datetime(timestamp, 'unixepoch', 'localtime') as ts
FROM messages
WHERE action IN ('WebFetch', 'WebSearch')
ORDER BY timestamp DESC
LIMIT :limit;
