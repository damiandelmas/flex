-- Add parent_uuid to _types_message for conversation fork reconstruction
-- Extracted from JSONL entry.parentUuid during sync_session_messages()
-- Safe: SQLite ALTER TABLE ADD COLUMN is non-destructive

ALTER TABLE _types_message ADD COLUMN parent_uuid TEXT;
