
create sequence raw_flexlm_app_snapshots_id_seq;
create table raw_flexlm_app_snapshots (id integer DEFAULT nextval('raw_flexlm_app_snapshots_id_seq'::regclass) PRIMARY KEY,
                                       for_date timestamp with time zone, feature varchar(50), 
                                       vendor varchar(50), total_licenses integer, used_licenses integer);
create sequence raw_flexlm_user_snapshots_id_seq;
create table raw_flexlm_user_snapshots (id integer DEFAULT nextval('raw_flexlm_user_snapshots_id_seq'::regclass) PRIMARY KEY,
                                        flexlm_app_snapshot_id integer references raw_flexlm_app_snapshots(id),
                                        username varchar(30),
                                        licenses integer,
                                        start timestamp with time zone,
                                        host varchar(30));
                                        
currval('raw_flexlm_app_snapshots_id_seq')
