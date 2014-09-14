-- Function: get_failed_v2()

-- DROP FUNCTION get_failed_v2();

CREATE OR REPLACE FUNCTION get_failed_v2()
  RETURNS integer AS
$BODY$			-- 获取时间 -- 每日  12：00 执行
	DECLARE 
		v_b_date_c  varchar(25); 	 --(beagin)开始时间
		v_e_date_c  varchar(25);         --(end)结束时间
		sql_cmd  varchar(1500);
		v_b_date_d      timestamp;       -- 计库 开始时间时间类型
	BEGIN 

        SELECT  (max(r_time)::date )::text  FROM  a_t_system_failed  INTO v_b_date_c;
	SELECT  (CURRENT_DATE)::text  INTO  v_e_date_c;
	SELECT  (max(r_time)::date ) FROM a_t_system_failed INTO v_b_date_d;
	

    sql_cmd =$s$ INSERT INTO  a_t_system_failed(md5,crc32,squence_id ) 
                SELECT md5,crc32,squence_id  FROM t_src_file where time_insert >= $s$
         || quote_literal( v_b_date_c) ||
        $s$  and  time_insert < $s$  ||  quote_literal(v_e_date_c)||
	$s$  and  batch_id not like  '%_force'
	     and  batch_id not like  '%_failed'
	     and  isrubbish in (-1,0)
	     and  isuniq in (-1,1)
	     and  derivatedfrom = ''
	     and  md5 not  in(

                select md5 FROM dblink (
                     'host=192.168.35.2 dbname=postgres_learn user=postgres port=5432  password=pg',
                     'SELECT md5 FROM final_data where r_time > ''
                     $s$ ||  v_b_date_d || $s$ '''
                 )
                        as t_0 (md5 varchar(32))
               ); $s$;	
	EXECUTE  sql_cmd;
	
    RETURN 0;
	END
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
ALTER FUNCTION get_failed_v2()
  OWNER TO postgres;

