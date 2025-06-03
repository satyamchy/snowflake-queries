create or replace table mytable5(
    student_id int,
    student_name string,
    department string,
    score varchar
    );
insert into mytable5 values
    (1,'john','cse','O'),
    (2,'saket','cse','O'),
    (3,'shamb','bba','A+'),
    (4,'sam','bca','B'),
    (5,'adi','bca','A');
 
select * from mytable5;



CREATE OR REPLACE FUNCTION CHECK_STUDENT(student_id INT)
RETURNS STRING
LANGUAGE SQL
AS
$$
    IFF(
        EXISTS (
            SELECT count(1) FROM mytable5 WHERE student_id = student_id
        ),
        'exists',
        'not exist'
    )
$$;
SELECT CHECK_STUDENT(1);

CREATE OR REPLACE PROCEDURE CHECK_TABLE2(student_id_p INT )
RETURNS STRING 
LANGUAGE SQL
AS
$$
DECLARE 
    tbl_count INTEGER;
BEGIN
    SELECT count(1) into :tbl_count FROM mytable5 WHERE student_id = student_id_p;

    IF :tbl_count > 0 
    RETURN 'exists';
    ELSE 
    RETURN 'not exist';
    END IF;
END;
$$;

CREATE OR REPLACE PROCEDURE CHECK_TABLE2(p_student_id INT)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE 
    tbl_count INTEGER;
BEGIN
    SELECT COUNT(1)
    INTO tbl_count
    FROM mytable5 
    WHERE student_id = p_student_id;

    IF tbl_count > 0  
        RETURN 'exists';
    ELSE 
        RETURN 'not exist';
    END IF;
END;
$$;

SELECT CHECK_STUDENT2(1);




CREATE OR REPLACE PROCEDURE CHECK_TABLE(tbl VARCHAR)
RETURNS STRING
LANGUAGE SQL
AS
$$

DECLARE 
    tbl_count INTEGER;
BEGIN
    SELECT COUNT(*) 
    INTO :tbl_count 
    FROM information_schema.tables 
    WHERE table_name = UPPER(:tbl)
      AND table_schema = CURRENT_SCHEMA();

    IF :tbl_count > 0  
        RETURN 'exists';
    ELSE 
        RETURN 'not exist';
    END IF;
END;
$$;

CALL CHECK_TABLE('mytable1');