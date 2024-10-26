CREATE TABLE public.files (
	id serial NOT NULL,
	filename text NOT NULL,
	status varchar NOT NULL,
	CONSTRAINT files_pk PRIMARY KEY (filename)
);