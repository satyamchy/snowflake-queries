use database mydb;
use schema sample_schema;

-- drop table customer_csv;
create or replace table employees(
    employee_id number(10),
    first_name varchar(20),
    last_name varchar(20),
    email varchar(20),
    phone_no varchar(30),
    hire_date date,
    job_id varchar(20),
    salary number(20),
    commission_pct varchar(20),
    manager_id number(20),
    department_id number(20)
);
show tables;
select * from employees;
select first_name, salary from employeess;


SELECT department_id, max(salary) FROM employees GROUP BY department_id;

SELECT department_id,salary, max(salary) FROM employees GROUP BY department_id, salary order by department_id;  -- ❌ Error: 'salary' must be in GROUP BY or used in aggregation 

SELECT department_id, employee_id, salary FROM employees ORDER BY department_id, salary DESC;
-- same same up and down
SELECT employee_id, department_id, salary FROM employees where department_id in (select distinct department_id from employees ) order by department_id; --❌ time complexity very high n*m

SELECT department_id, employee_id, salary FROM employees ORDER BY department_id, salary ;
SELECT employee_id, department_id,salary FROM employees where department_id = 50;


select  department_id, salary , ROW_NUMBER() OVER( partition by department_id order by salary desc ) as rank from employees; 

SELECT department_id, salary
FROM (
    SELECT department_id, salary,
           ROW_NUMBER() OVER(PARTITION BY department_id ORDER BY salary DESC) AS rnk
    FROM employees
) AS ranked
WHERE rnk = 1
ORDER BY department_id;




SELECT department_id, SUM(salary) FROM employees where department_id is not null GROUP BY department_id;

SELECT department_id, SUM(salary) FROM employees WHERE department_id IS NOT NULL GROUP BY department_id
HAVING COUNT(*) >= 1;


--==========================================================
truncate table employees;
create or replace stage sample_stage2;
create or replace file format employees_ff
    type = 'csv',
    compression = 'none'
    field_delimiter = ','
    skip_header = 1;

copy into employees 
    from 
    @sample_stage2
    file_format = employees_ff
    on_error = 'continue';

create or replace table emp_tab as 
    select * from employees where salary >= 3000;

select * from employees;
select * from emp_tab;

select count(*) from employees
select count(*) from emp_tab;
--=========================================
select  employee_id from employees
union all
select employee_id from emp_tab;

select  employee_id from employees
intersect 
select employee_id from emp_tab;

select  employee_id from employees
except 
select employee_id from emp_tab;
--============================================================
select max(salary)  from employees;
select * from employees where salary = (select max(salary)  from employees);
select first_name,  max(salary) over() as max_salary from employees;

select first_name from employees where salary in (select  max(salary) from employees group by department_id);

-- 2nd max salary  =======================
select max(salary) from employees where salary <> (select max(salary) from employees);

select distinct salary from employees order by salary desc limit 1 offset 1;

select distinct department_id from employees;
SELECT salary, DENSE_RANK() OVER (ORDER BY salary DESC) AS rnk FROM employees;

SELECT salary
    FROM (
      SELECT salary, DENSE_RANK() OVER (ORDER BY salary DESC) AS rnk
      FROM employees
    ) AS ranked
    WHERE rnk = 2;


--=======================================================

select first_name, salary,
    ROW_NUMBER() over(order by salary desc) as row_number
    from employees;
    
select first_name, salary,
    
    DENSE_RANK() over(order by salary) as dense_rank,
    from employees;

select * , ROW_NUMBER() OVER( partition by department_id order by salary desc) as rank from employees;

select * from (
    select first_name, salary, ROW_NUMBER() OVER( partition by department_id order by salary desc) as rank from employees
) where rank = 1;

select first_name, salary, department_id,
    ROW_NUMBER() over(partition by department_id order by salary desc) as row_number,
    RANK() over (order by salary desc) as rank_, 
    DENSE_RANK() over(order by salary desc) as dense_rank,
    from employees;

select first_name, salary, employee_id, department_id , hire_date, 
    LAG(hire_date) over(order by hire_date) as lag_hire_date,
    LEAD(hire_date) over(order by hire_date) as lead_hire_date
    from employees;
select first_name, salary, employee_id, department_id , hire_date, 
    LAG(hire_date,3) over(order by hire_date) as lag_hire_date,
    LEAD(hire_date,3) over(order by hire_date) as lead_hire_date
    from employees;
select first_name, salary, employee_id, department_id , hire_date, 
    LAG(hire_date,3, current_date) over(order by hire_date) as lag_hire_date,
    LEAD(hire_date,3, current_date) over(order by hire_date) as lead_hire_date
    from employees; -- fill current_date in null values

select first_name, salary, employee_id, department_id , hire_date, 
    LAG(hire_date) over( partition by department_id order by hire_date) as lag_hire_date,
    LEAD(hire_date) over(partition by department_id order by hire_date) as lead_hire_date
    from employees; 

    
--========================================================
--==================================================================================================
select UPPER('snowflakelake') as result;
select LOWER('DRAMA') as result;
select SUBSTR('cauliflower', 5,5) as result;
select instr('snowflake', 'ake') as result; --============*********
select 
    LPAD('saf123', 15, 0) as left_padding,
    RPAD('123', 15, 0) as right_padding;
select 
    LTRIM('      saf123     ') as left_trim,
    RTRIM('sam123         ') as right_trim;

select SUBSTR('cauliflower', 5) as result;
select replace('25-01-20', '-', '/') as result;
select translate('123-456-7890', '1234567890', 'abcdefghij') AS result;
select NVL( null, 'default') as result;
select NULLIF(10, 10) as result;
select coalesce(null, null, 'snowflake') as result;
select round(134.45354, 2) as result;

select trunc(DATE '2025-05-21', 'MONTH') as result;
SELECT MONTHS_BETWEEN(DATE '2025-05-21', DATE '2025-01-01') AS result;
SELECT TO_DATE('2025-05-21', 'YYYY-MM-DD') AS result;
SELECT NEXT_DAY(DATE '2025-05-21', 'MONDAY') AS result;
SELECT TO_CHAR(DATE '2025-05-21', 'YYYY/MM/DD') AS result;
SELECT TO_CHAR(12345.678, '99999.99') AS result;


---========================================================================



