------------- Main info -------------
-- Filename: get_curr_files.sql
-- Date: 22.10.2024
-- Description: 
-------------------------------------

DELETE FROM iquam_files AS iquam
WHERE iquam.id IN ($id_on_del)