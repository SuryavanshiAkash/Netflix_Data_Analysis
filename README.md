# Netflix_Data_Analysis

 ELT Workflow:

Loaded the CSV into MySQL using Python (SQLAlchemy + create_engine).

Applied data cleansing and structuring using SQL.

Normalized the dataset by breaking multi-valued columns (genre, director, cast, country) into separate relational tables using recursive CTEs.

🔹 SQL Logic Highlights:

Removed duplicates using window functions (row_number()).

Converted textual date formats (date_added) to proper SQL DATE.

Handled missing data and enriched columns using smart joins.

Created a netflix_staging table for clean and ready-to-analyze data.

🔹 Insights Extracted:

Count of movies vs TV shows for each director.

Top countries producing comedy content.

Year-wise directors with the highest number of movie releases.

Average duration of movies in each genre.

Identified directors who have worked in both horror and comedy movies.
