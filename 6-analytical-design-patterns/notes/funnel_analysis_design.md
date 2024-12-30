# FUNNEL ANALYSIS

#### DE Interview that gets it right

- Care about number of table scans
    - COUNT(CASE WHEN) very powerful combo during iv and on job
    - Cumulative table design minimizes table scans
- Clean SQL code
    - Using CTE
        - Some edge cases with old db version or different db where performance might hurt
    - Using aliases in column esp for derived column
- Use EXPLAIN more
    - Abstract syntax tree- hierarchical order plan of sql

#### Advanced SQL Techniques to Try Out

- GROUPING SETS / GROUP BY CUBE / GROUP BY ROLLUP
- Self-joins
- Window Functions - LAG(), LEAD(), ROWS()
- CROSS JOIN UNNEST / LATERAL VIEW EXPLODE
    - Almost the same, cross join is more like db, where lateral view is more on the big data tools

#### GROUPING SETS

- GROUP BY GROUPING SETS(x,y) is roughly equivalent to GROUP BY x UNION ALL GROUP BY y
- UNION ALL hurts performance, one of the slowest keyword
    - Readability - dont have to copy paste multiple time

    -   ```sql
        -- Using GROUPING SETS
        SELECT 
            department,
            SUM(salary) as total_salary
        FROM employees
        GROUP BY GROUPING SETS (
            (department),  -- Group by department
            ()            -- Grand total
        );

        -- Same result using UNION ALL
        SELECT department, SUM(salary) as total_salary
        FROM employees
        GROUP BY department
        UNION ALL
        SELECT NULL, SUM(salary)
        FROM employees;
        ```
- MAKE SURE column is never null in GROUPING SETS for excluded columns
    - Use COALESCE to make it not NULL
    - Because if it is not, you will never know if the data is NULL because it is excluded, or it is really NULL

#### CUBE

- Gives you all possible permutation for grouped dimension

    -   ```sql
        -- Using CUBE
        SELECT 
            department,
            job_role,
            SUM(salary) as total_salary
        FROM employees
        GROUP BY CUBE(department, job_role);
        ```

- Results
    department | job_role | total_salary
    -----------+----------+-------------
    HR         | Manager  | 50000        -- HR Managers
    HR         | Staff    | 40000        -- HR Staff
    IT         | Manager  | 60000        -- IT Managers
    IT         | Staff    | 45000        -- IT Staff
    HR         | NULL     | 90000        -- All HR
    IT         | NULL     | 105000       -- All IT
    NULL       | Manager  | 110000       -- All Managers
    NULL       | Staff    | 85000        -- All Staff
    NULL       | NULL     | 195000       -- Grand total