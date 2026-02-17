-- @name: rename-claude-to-claude-chat
-- @description: Standardize cell name. claude_chat pairs with claude_code. Symmetric naming.
-- @target: registry

-- In registry.db (not the cell DB):
UPDATE cells SET name = 'claude_chat' WHERE name = 'claude';
