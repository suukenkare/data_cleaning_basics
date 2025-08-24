-- Cleaning Plan
/*
1. Create Copy of Table To Avoid Altering Original Table
2. Remove Duplicates
3. Standardize Data
4. Remove NULLS, Unknown etc
5. Remove Unwanted Columns If Any
*/

-- 1. Create Copy of Table To Avoid Altering Original Table

	-- Create table - layoffs_staging
		CREATE TABLE layoffs_staging LIKE layoffs;

	-- Insert values into layoffs_staging
		INSERT INTO layoffs_staging
        SELECT * FROM layoffs;
        
	
-- 2. Remove Duplicates

	SELECT * FROM layoffs_staging;

	-- Add row count to check for duplicates
		SELECT *,
		ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country,funds_raised_millions) AS row_num
		FROM layoffs_staging;
        
	-- To filter based on row_num use CTE (Common Table Expression) - Optional for analysis
		WITH dupe_cte AS
        (
			SELECT *,
			ROW_NUMBER() OVER (
            PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country,funds_raised_millions) AS row_num
			FROM layoffs_staging
        )
        SELECT *
        FROM dupe_cte 
        WHERE row_num > 1;
        
	-- Confirming these are actual duplicates
		SELECT * FROM layoffs_staging
        WHERE company = 'Casper';

	-- CTEs do not allow update functions like DELETE etc. So adding row_num column to a copy of layoffs_staging table so I can delete dupe rows
		CREATE TABLE `layoffs_staging2` (
					  `company` text,
					  `location` text,
					  `industry` text,
					  `total_laid_off` text,
					  `percentage_laid_off` text,
					  `date` text,
					  `stage` text,
					  `country` text,
					  `funds_raised_millions` text,
					  `row_num` INT
					) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
                    
		SELECT * FROM layoffs_staging2;
        
        -- Inserting all values from layoffs_staging + row_num field to layoffs_staging2
			INSERT INTO layoffs_staging2
			SELECT *,
			ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country,funds_raised_millions) AS row_num
			FROM layoffs_staging;
			
			SELECT *
			FROM layoffs_staging2
			WHERE row_num > 1;
        
        -- Disable MySQL Workbench Safe Update Mode in case of Error Code 1175.
			SET SQL_SAFE_UPDATES = 0; 
        
        -- Delete duplicate rows
			DELETE FROM layoffs_staging2 WHERE row_num > 1;
        
        
			SELECT * 
			FROM layoffs_staging2
			WHERE row_num >1; -- Returns 0 rows as expected. Duplicates are deleted
        
-- 3. Standardize Data

	-- Trimming Data
		SELECT company,TRIM(company)
		FROM layoffs_staging2;
		
		UPDATE layoffs_staging2
		SET company = TRIM(company);
        
        SELECT DISTINCT industry
        FROM layoffs_staging2
        ORDER BY 1;
        
	-- Found three entries - 'Crypto Currency', 'CryptoCurrency' and 'Crypto' - Standardize this
        
        SELECT *
        FROM layoffs_staging2
        WHERE industry LIKE 'Crypto%';
        
        UPDATE layoffs_staging2
        SET industry = 'Crypto'
        WHERE industry LIKE 'Crypto%';
		
        
        SELECT DISTINCT country
        FROM layoffs_staging2
        ORDER BY 1;
        
	-- Theres an entry with 'United States.' - Remove '.'
        
        SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
        FROM layoffs_staging2
        ORDER BY 1;
        
        UPDATE layoffs_staging2
        SET country = TRIM(TRAILING '.' FROM country)
        WHERE country LIKE 'United States%';
        
        
	-- Date was imported as Text. Change the data type for future analysis
        SELECT DISTINCT `date` FROM layoffs_staging2
        ORDER BY 1;
        
        -- Date is in M/D/YYYY Format
			SELECT `date`, str_to_date(`date`,'%m/%d/%Y')
			FROM layoffs_staging2;
        
        -- There was a 'NULL' in date field. Handling that before str_to_date() to avoid Error Code 1141
        
			SELECT `date` FROM layoffs_staging2
			WHERE `date` = "NULL";
			
			UPDATE layoffs_staging2
			SET `date` = '1/1/1800'
			WHERE `date` = "NULL";
			
			UPDATE layoffs_staging2
			SET `date` = str_to_date(`date`,'%m/%d/%Y');
			
		-- After changing format, Date is still text. Update Data Type
			ALTER TABLE layoffs_staging2
			MODIFY COLUMN `date` DATE;
        
-- 3. Remove Nulls/Unknowns - Fill missing information

	-- Populating missing industry. 
    
		SELECT * FROM layoffs_staging2
		WHERE industry = 'NULL' OR industry = '';

    -- Use Self Join
    
		SELECT t1.industry, t2.industry
		FROM layoffs_staging2 t1
		JOIN layoffs_staging2 t2
		ON t1.company = t2.company
		WHERE (t1.industry IS NULL OR t1.industry = '')
		AND t2.industry IS NOT NULL;
		
		UPDATE layoffs_staging2
		SET industry = NULL
		WHERE industry = '';
    
    -- Below script did not work the first time. All blanks in t1 should be changed to null.
		UPDATE layoffs_staging2 t1
		JOIN layoffs_staging2 t2
		ON t1.company = t2.company
		SET t1.industry = t2.industry
		WHERE t1.industry IS NULL
		AND t2.industry IS NOT NULL;
    
-- Additional Updates
    
	-- Updating all blanks and "NULL" to NULL markers
		SELECT *
		FROM layoffs_staging2;
		-- WHERE location = "NULL" or location IS NULL or location ='';
    
		UPDATE layoffs_staging2
		SET percentage_laid_off = NULL
		WHERE percentage_laid_off = 'NULL' OR percentage_laid_off = '';
		
		UPDATE layoffs_staging2
		SET funds_raised_millions = NULL
		WHERE funds_raised_millions = 'NULL' OR funds_raised_millions = '';
    
    -- Change Data Types of Numeric Fields Accordingly.
    
		ALTER TABLE layoffs_staging2
		MODIFY COLUMN total_laid_off int;
		   
		ALTER TABLE layoffs_staging2
		MODIFY COLUMN percentage_laid_off float;
		
		ALTER TABLE layoffs_staging2
		MODIFY COLUMN funds_raised_millions float;
    
-- DELETE row_num column
    
    ALTER TABLE layoffs_staging2
    DROP COLUMN row_num;
    
    
    
	
    
    
    
        
        

        
        
        

