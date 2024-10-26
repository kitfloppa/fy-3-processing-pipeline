------------- Main info -------------
-- Filename: get_remote_files.sql
-- Date: 26.10.2024
-- Description: 
-------------------------------------

UPDATE files
SET files.status = 'RT_STPS'
WHERE files.filename = '$filename'