-- @name: standardize-sessions-view
-- @description: Rename conversations view to sessions. Standardize source-level view name across claude_code and claude_chat cells.
-- @target: _meta, views

-- Remove old view level key
DELETE FROM _meta WHERE key = 'view:conversations:level';

-- Add new view level key
INSERT OR REPLACE INTO _meta (key, value) VALUES ('view:sessions:level', 'source');

-- Drop stale view (regenerate_views doesn't clean views it didn't create)
DROP VIEW IF EXISTS conversations;

-- Update description
INSERT OR REPLACE INTO _meta (key, value)
VALUES ('description', 'Claude.ai conversation archive. Each source is a conversation, each chunk is a message (user prompt or assistant response). Views: messages (chunk-level), sessions (source-level). Temporal dimension from folder structure. ~32K chunks, ~2K conversations.');

-- Then: regenerate_views(db) from Python
