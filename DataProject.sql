SELECT * 
FROM layoffs;

-- Create copy of layoffs

CREATE TABLE layoffs_staging
LIKE layoffs;

INSERT layoffs_staging
SELECT *
FROM layoffs;

SELECT *
FROM layoffs_staging
;

-- Remove duplicates


-- Create CTE to find duplicate by using ROW_NUMBER
WITH cte AS
(
SELECT *,
ROW_NUMBER() OVER(
	PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
    ) AS row_num
FROM layoffs_staging
)
SELECT * 
FROM cte
WHERE row_num > 1;

-- Create new table

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` double DEFAULT NULL,
  `percentage_laid_off` double DEFAULT NULL,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` double DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(
	PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
    ) AS row_num
FROM layoffs_staging;

SELECT * FROM layoffs_staging2 
WHERE row_num > 1;

DELETE 
FROM layoffs_staging2 
WHERE row_num > 1;

SET SQL_SAFE_UPDATES = 0;

-- Finished remove duplication, step 2: standardize data

-- TRIM to cut of the blank space
SELECT DISTINCT(company), TRIM(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);

-- Move any industry like Crypto into Crypto
SELECT DISTINCT(industry) 
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';


-- United State.  Need to remove the .
SELECT DISTINCT(country)
FROM layoffs_staging2;

SELECT DISTINCT(country), TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY 1
;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country);

-- update date into date type

SELECT `date` , STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_staging2
;

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

ALTER TABLE layoffs_staging2
MODIFY `date` DATE;

-- CLEANING NULL AND BLANK

SELECT * 
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL
;

SELECT * 
FROM layoffs_staging2
WHERE company = 'Airbnb'
;

UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = ''
;

SELECT * 
FROM layoffs_staging2 as t1
 JOIN layoffs_staging2 as t2
 ON t1.company = t2.company
 WHERE (t1.industry IS NULL OR t1.industry = '')
 AND t2.industry IS NOT NULL;
 
UPDATE layoffs_staging2 as t1 
	JOIN layoffs_staging2 as t2	
	ON t1.company = t2.company
SET t1.industry = t2.industry
 WHERE t1.industry IS NULL
 AND t2.industry IS NOT NULL
 ;
 
 
 -- STEP 4: DELETE UNNESSECARY ROWS
 
DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT * 
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

SELECT * 
FROM layoffs_staging2;

-- DONE

-- DATA ANAYLYTICS


-- The max laid off number
SELECT MAX(total_laid_off), MAX(percentage_laid_off)
FROM layoffs_staging2;


-- company with 100% laid off
SELECT * 
FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY total_laid_off DESC;


-- the company with the most laid off
SELECT company, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC
;


-- The period the the laid off data
SELECT MIN(`date`), MAX(`date`)
FROM layoffs_staging2;


-- The industry with the most laid off
SELECT industry, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY industry
ORDER BY 2 DESC
;


-- The country with the most laid off
SELECT country, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY country
ORDER BY 2 DESC
;


-- The year with the most laid off
SELECT YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY YEAR(`date`)
ORDER BY 1 DESC
;

-- the stage that has the most laid off
SELECT stage, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY stage
ORDER BY 2 DESC
;
 
SELECT SUBSTRING(`date`, 1, 7) AS `MONTH`, SUM(total_laid_off)
FROM layoffs_staging2
WHERE SUBSTRING(`date`, 1, 7) IS NOT NULL
GROUP BY `MONTH`
ORDER BY 1 ASC;


-- Rolling total of laid_off in each month
WITH Rolling_total AS
(
	SELECT SUBSTRING(`date`, 1, 7) AS `MONTH`, SUM(total_laid_off) as total_off
	FROM layoffs_staging2
	WHERE SUBSTRING(`date`, 1, 7) IS NOT NULL
	GROUP BY `MONTH`
	ORDER BY 1 ASC
)
SELECT `MONTH`,
total_off,
 SUM(total_off) OVER(ORDER BY `MONTH`) as rolling_total
FROM Rolling_total
;


-- Top 5 most laid_off company based on each year
WITH Company_Years (company, years, total_laid_off) AS
(
	SELECT company, YEAR(`date`), SUM(total_laid_off)
    FROM layoffs_staging2
    GROUP BY company, YEAR(`date`)
), Company_Years_Rank AS
	(
		SELECT *,
        DENSE_RANK() OVER(PARTITION BY years ORDER BY total_laid_off DESC) AS Ranking
        FROM Company_Years
        WHERE years IS NOT NULL
    )
    SELECT * 
    FROM Company_Years_Rank
    WHERE Ranking <= 5;







