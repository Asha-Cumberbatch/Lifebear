# Data Cleaning Script
![datacleaning](datacleaning.jpg)
## Overview

This project is a data cleaning tool that processes CSV data, specifically removing invalid entries, cleaning up fields, and identifying invalid rows. The script reads data in chunks, cleans each chunk, and saves both cleaned data and garbage data (removed rows) for review. The project is designed to process large CSV files efficiently by dividing them into smaller chunks and handling invalid data, especially focusing on cleaning emails and essential fields.

## Key Features

- **Chunk-Based Processing:** The script splits large CSV files into smaller chunks for easier processing and memory management.
- **Email Validation:** It uses a simplified regex to validate email addresses, ensuring only valid emails are retained.
- **Garbage Data Collection:** Rows with missing essential data (like `login_id`, `mail_address`, and `password`) or invalid email addresses are categorized as garbage data.
- **Data Deduplication:** Duplicate records are removed, ensuring only unique entries are saved in the final cleaned file.
- **User-Defined Paths:** The script allows flexible input and output paths for easy file management.
- **Rate Calculation:** After processing each chunk, the script calculates the ratio of cleaned data to garbage data.

## File Structure

```text
.
├── DataCleaning/
│   ├── Lifebear.csv                # Original dataset
│   ├── Chunks/                     # Folder containing original data chunks
│   ├── Cleaned_Chunks/             # Folder containing cleaned data chunks
│   ├── Garbage/                    # Folder containing garbage (invalid) data chunks
│   ├── merged_cleaned.csv          # File containing the merged cleaned data from all chunks
│   └── merged_garbage.csv          # File containing the merged garbage data from all chunks
├── data_cleaning_script.py         # The main data cleaning script
└── README.md                       # This documentation
```

## How It Works

1. **Setup Directories:** The script first ensures the output directories exist for cleaned data, garbage data, and the original chunks.
   
2. **Read CSV Data in Chunks:** The CSV file is read in chunks, with each chunk being processed individually. The number of chunks is customizable.

3. **Data Cleaning:**
   - Rows containing invalid patterns (like consecutive commas) or invalid emails are removed.
   - Essential fields (`login_id`, `mail_address`, and `password`) are checked for missing data, and rows with all missing values are considered invalid.
   - Email validation uses a regular expression to ensure the email has a proper format.

4. **Garbage Data Collection:** Rows that do not pass validation (either due to missing essential fields or invalid emails) are stored in a garbage DataFrame for each chunk.

5. **Merging Data:** After processing all chunks, the cleaned data and garbage data from each chunk are merged, and duplicates are removed.

6. **Final Output:** 
   - Cleaned data is saved to `merged_cleaned.csv`.
   - Garbage data is saved to `merged_garbage.csv`.

## How to Run the Script

### Requirements

- Python 3.x
- Pandas Library
- Regular Expressions (`re`)
- OS Module (`os`)

To install Pandas, run:

```bash
pip install pandas
```

### Usage

1. Place the CSV file to be cleaned in the specified folder (in this case, `DataCleaning/Lifebear.csv`).
2. Modify the paths in the script (`csv_file_path`, `before_cleaning_folder`, `cleaned_chunks_folder`, `garbage_folder`, etc.) to point to the correct locations on your system.
3. Run the script:

```bash
python data_cleaning_script.py
```

4. After running, check the `Cleaned_Chunks` folder for cleaned data and the `Garbage` folder for garbage data. The final merged cleaned and garbage files will be saved as `merged_cleaned.csv` and `merged_garbage.csv`, respectively.

## Notes

- The chunk size is determined automatically based on the total number of rows in the CSV file and the number of chunks (`num_chunks`) you define.
- The script assumes that the CSV file uses a semicolon (`;`) as a delimiter. Adjust the delimiter if necessary.
- The email validation is simplified to focus on general formatting. Additional validation can be implemented if needed.


## Credits
- Asha Cumberbatch
