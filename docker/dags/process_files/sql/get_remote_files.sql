------------- Main info -------------
-- Filename: get_remote_files.sql
-- Date: 23.10.2024
-- Description: 
-------------------------------------

SELECT f."filename" FROM files AS f
WHERE f."status" = 'NOT_DOWNLOADED'
LIMIT 1