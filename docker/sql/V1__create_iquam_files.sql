CREATE TABLE public.iquam_files (
	id serial4 NOT NULL,
	filename text NOT NULL,
	status varchar NOT NULL,
	created_date timestamp NOT NULL,
	CONSTRAINT iquam_files_pk PRIMARY KEY (filename)
);