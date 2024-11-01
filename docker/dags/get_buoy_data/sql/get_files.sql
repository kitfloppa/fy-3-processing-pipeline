------------- Main info -------------
-- Filename: get_files.sql
-- Date: 27.10.2024
-- Description: 
-------------------------------------

SELECT iquam_files."id",
       iquam_files."filename"
FROM iquam_files
WHERE iquam_files."status" = 'NOT_DOWNLOADED'
LIMIT 1