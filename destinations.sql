drop view flexlm_app_snapshots;
drop view flexlm_user_snapshots;

drop table raw_flexlm_app_snapshots;
drop table raw_flexlm_user_snapshots;

drop sequence raw_flexlm_app_snapshots_id_seq;
drop sequence raw_flexlm_user_snapshots_id_seq;

drop table raw_collectl_executions;
drop sequence raw_collectl_executions_id_seq;

drop table collectl_executables;
drop sequence collectl_executables_id_seq;

drop table collectl_executions;
drop sequence collectl_executions_id_seq;

--

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

create view flexlm_app_snapshots as select * from raw_flexlm_app_snapshots;
create view flexlm_user_snapshots as select * from raw_flexlm_user_snapshots;
                                        
create sequence raw_collectl_executions_id_seq;
create table raw_collectl_executions(id integer DEFAULT nextval('raw_collectl_executions_id_seq'::regclass) PRIMARY KEY,
                                     START_TIME timestamp with time zone, 
                                     END_TIME timestamp with time zone,
                                     PID integer,
                                     UID integer,
                                     EXECUTABLE varchar(200),
                                     HOST varchar(30));

create sequence collectl_executables_id_seq;
create table collectl_executables(id integer DEFAULT nextval('collectl_executables_id_seq'::regclass) PRIMARY KEY,
                                  NAME varchar(200),
                                  RESOURCE_ID integer REFERENCES sw.resources(id));

create sequence collectl_executions_id_seq;
create table collectl_executions(id integer DEFAULT nextval('collectl_executions_id_seq'::regclass) PRIMARY KEY,
                                 START_TIME timestamp with time zone,
                                 END_TIME timestamp with time zone,
                                 PID integer,
                                 HOST varchar(30),
                                 COLLECTL_EXECUTABLE_ID integer REFERENCES collectl_executables(id),
                                 USER_ID integer REFERENCES people.users(id));
