CREATE TABLE public.otp_log (
	ip_address varchar(15) NULL,
	status_code int4 NULL,
	payload text NULL,
	created_at timestamptz NULL DEFAULT CURRENT_TIMESTAMP
);

ALTER TABLE otp_log ADD CONSTRAINT unix_otp_log UNIQUE (created_at, ip_address, status_code, payload);
