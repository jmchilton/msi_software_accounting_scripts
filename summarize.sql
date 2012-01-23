select count(*) as the_count, executable from raw_collectl_executions group by executable order by the_count desc;
