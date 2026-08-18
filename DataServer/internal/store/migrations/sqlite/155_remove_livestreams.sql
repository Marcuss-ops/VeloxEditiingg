-- 155_remove_livestreams.sql
--
-- Livestream management was never part of the canonical Velox execution
-- domain. Older builds could create this table from runtime store methods;
-- remove that orphaned state while retiring the code and HTTP surface.
DROP TABLE IF EXISTS livestreams;
