# Real Estate ETL

Real Estate ETL is an **ETL (Extract, Transform, Load)** pipeline designed to process property sales data from multiple counties into a PostgreSQL database. It supports Osceola County with Excel (XLS/XLSX) input and is extensible for other counties with different formats (e.g., JSON). All data is loaded into a single `property_sales` table with a `county_code` column to distinguish records by county.

## Features

- **Flexible Input Formats**: Processes Excel for Osceola and JSON for other counties, with an extensible extraction layer.
- **Unified Table**: Stores all county data in `public.property_sales` with nullable columns and a `county_code`.
- **Data Validation**: Ensures numeric (e.g., `bath` within `[-99.99, 99.99]`), integer (e.g., `yr_roll`), and text field integrity.
- **Security**: Uses environment variables, parameterized queries, minimal permissions, and secure logging.
- **Modular Design**: Separates Extract, Transform, Load, Config, and Utils for maintainability.
- **Unit Tests**: Includes comprehensive tests for all components using `pytest`.

## Directory Structure

```
realestateetl/
├── src/
│   ├── extract/
│   │   ├── __init__.py
│   │   ├── excel_extractor.py
│   │   ├── json_extractor.py
│   │   └── base_extractor.py
│   ├── transform/
│   │   ├── __init__.py
│   │   └── transformer.py
│   ├── load/
│   │   ├── __init__.py
│   │   └── postgres_loader.py
│   ├── config/
│   │   ├── __init__.py
│   │   ├── osceola_config.py
│   │   └── county_configs.py
│   └── utils/
│       ├── __init__.py
│       └── utils.py
├── tests/
│   ├── extract/
│   │   ├── __init__.py
│   │   ├── test_excel_extractor.py
│   │   └── test_json_extractor.py
│   ├── transform/
│   │   ├── __init__.py
│   │   └── test_transformer.py
│   ├── load/
│   │   ├── __init__.py
│   │   └── test_postgres_loader.py
│   ├── config/
│   │   ├── __init__.py
│   │   └── test_county_configs.py
│   └── utils/
│       ├── __init__.py
│       └── test_utils.py
├── main.py
├── .env
├── .gitignore
├── pytest.ini
├── README.md
├── requirements.txt
```

## Prerequisites

- Python 3.8+
- PostgreSQL 12+
- Git (for version control)

## Setup

1. **Clone the Repository**:

   ```bash
   git clone https://github.com/your-username/Real Estate ETL.git
   cd Real Estate ETL
   ```

2. **Install Dependencies**:

   ```bash
   pip install -r requirements.txt
   ```

3. **Configure PostgreSQL**:

   - Create the `property_sales` table:

     ```sql
     CREATE TABLE public.property_sales (
         county_code VARCHAR(10),
         parcel_strap VARCHAR(50),
         property_address TEXT,
         date_sold DATE,
         price NUMERIC(15,2),
         book_pg VARCHAR(20),
         bed INTEGER,
         bath NUMERIC(4,2),
         pool BOOLEAN,
         sqft INTEGER,
         yos INTEGER,
         sellers TEXT,
         buyers TEXT,
         deed_code VARCHAR(10),
         mailing_address_1 TEXT,
         mailing_address_2 TEXT,
         mailing_address_3 TEXT,
         mailing_city VARCHAR(100),
         mailing_state VARCHAR(2),
         mailing_zip_code VARCHAR(10),
         mailing_country VARCHAR(100),
         site_city VARCHAR(100),
         site_zip VARCHAR(10),
         adj_price NUMERIC(15,2),
         dos DATE,
         price_per_sqft NUMERIC(10,2),
         nh_cd VARCHAR(10),
         neighborhood VARCHAR(100),
         sub VARCHAR(10),
         sub_division TEXT,
         gross_ar INTEGER,
         acreage NUMERIC(10,4),
         yr_roll INTEGER,
         dor_cd VARCHAR(10),
         dor_description TEXT,
         trns_cd VARCHAR(10),
         qu_flg VARCHAR(10),
         vi VARCHAR(10),
         grantors TEXT,
         all_grantees TEXT,
         or_book INTEGER,
         or_page INTEGER,
         high_school_id VARCHAR(10),
         high_school VARCHAR(100),
         middle_school_id VARCHAR(10),
         middle_school VARCHAR(100),
         elementary_school_id VARCHAR(10),
         elementary_school VARCHAR(100),
         street TEXT,
         sales_analysis_id VARCHAR(10),
         sales_ratio NUMERIC(8,2),
         rea_cd VARCHAR(10),
         rea_description TEXT,
         jst_val NUMERIC(15,2),
         ayb INTEGER,
         multi_parcel BOOLEAN,
         book_page VARCHAR(20),
         is_primary BOOLEAN,
         is_secondary BOOLEAN,
         market_area1 VARCHAR(10),
         market_area1_description TEXT,
         market_area2 VARCHAR(10),
         hotel_units INTEGER,
         apt_units INTEGER,
         PRIMARY KEY (county_code, parcel_strap)
     );
     ```

   - Grant minimal permissions:

     ```sql
     GRANT INSERT ON public.property_sales TO your_username;
     GRANT USAGE ON SCHEMA public TO your_username;
     ```

4. **Configure Environment**:

   - Create `.env` from the template:

     ```bash
     cp .env.example .env
     ```

   - Edit `.env` with your PostgreSQL credentials:

     ```
     DB_USER=your_username
     DB_PASSWORD=your_password
     DB_HOST=localhost
     DB_PORT=5432
     DB_NAME=your_database
     ```

## Usage

1. **Prepare Input File**:

   - **Osceola**: Provide an Excel file (`input.xlsx`) with headers matching `src/config/osceola_config.py` (e.g., `Parcel/Strap`, `Price`, `Bath`).
   - **Other Counties**: Provide a JSON file (e.g., `input.json`) with headers matching the county’s config.

2. **Run the ETL Pipeline**:

   ```bash
   python main.py
   ```

   - Update `main.py` to specify the county and file:

     ```python
     county = 'osceola'
     excel_file = 'input.xlsx'
     ```

   - Optionally, add command-line arguments:

     ```python
     import argparse
     parser = argparse.ArgumentParser()
     parser.add_argument('--county', default='osceola', help='County to process')
     parser.add_argument('--file', default='input.xlsx', help='Input file path')
     args = parser.parse_args()
     county = args.county
     excel_file = args.file
     ```

     Then run:

     ```bash
     python main.py --county osceola --file input.xlsx
     ```

## Running Tests

1. **Run Unit Tests**:

   ```bash
   pytest
   ```

2. **View Coverage**:

   ```bash
   pytest --cov=src --cov-report=html
   open htmlcov/index.html
   ```

The tests cover:
- Excel and JSON extraction
- Data transformation (numeric, integer, text, date)
- PostgreSQL loading (mocked)
- County configuration loading
- Utility functions

## Adding a New County

1. **Create Config**:

   - Copy `src/config/osceola_config.py` to `src/config/miami_dade_config.py`.
   - Update `county_code` and `column_mapping`:

     ```python
     CONFIG = {
         'county_code': 'MIAMI_DADE',
         'column_mapping': {
             'parcel_strap': 'ParcelID',
             'property_address': 'Address',
             'date_sold': 'SaleDate',
             'price': 'SalePrice',
             # Add other mappings
         },
         'schema_name': 'public',
         'table_name': 'property_sales',
         'table_columns': [...]  # Same as Osceola
     }
     ```

2. **Update Extractor (if needed)**:

   - For JSON input, modify `src/extract/json_extractor.py` to handle the county’s JSON structure.
   - Example JSON:

     ```json
     [
         {"ParcelID": "123", "Address": "123 Main St", "SaleDate": "2023-01-01", "SalePrice": 250000},
         ...
     ]
     ```

3. **Run for New County**:

   - Update `main.py` or use command-line arguments:

     ```bash
     python main.py --county miami_dade --file input.json
     ```

## Security

- **Credentials**: Stored in `.env`, excluded via `.gitignore`.
- **SQL Injection**: Prevented by `psycopg2` parameterized queries.
- **Permissions**: Minimal `INSERT` privileges enforced.
- **Logging**: No sensitive data exposed.
- **Validation**: Input data sanitized to prevent malicious inputs.

## Troubleshooting

- **Logs**: Check `INFO`, `WARNING`, `ERROR` messages in the console.
- **Common Issues**:
  - **Missing `.env`**: Ensure `.env` exists with valid credentials.
  - **Table Schema**: Verify `county_code` column and nullable columns.
  - **Input File**: Ensure headers match the county’s `column_mapping`.
  - **Tests**: Check `pytest` output for failures; ensure `pytest-cov` is installed.
- **Debugging**:
  - Share logs, input file sample (headers and 1-2 rows), and county details.
  - For test failures, share `pytest` output.

## Extending the Project

- **New File Formats**: Add extractors in `src/extract/` (e.g., `csv_extractor.py`).
- **Custom Validation**: Update `src/transform/transformer.py` or add county-specific rules in configs.
- **CI/CD**: Add GitHub Actions for automated testing:

  ```yaml
  name: Run Tests
  on: [push, pull_request]
  jobs:
    test:
      runs-on: ubuntu-latest
      steps:
      - uses: actions/checkout@v3
      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.8'
      - name: Install dependencies
        run: pip install -r requirements.txt
      - name: Run tests
        run: pytest --cov=src --cov-report=xml
  ```

## License

MIT License (or specify your preferred license).
