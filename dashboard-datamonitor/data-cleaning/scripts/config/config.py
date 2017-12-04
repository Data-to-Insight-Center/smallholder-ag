[smallholder]
extension = ".xlsx"
data_files_dir_path = "../data-files/"
data_files_output_dir_path = "../data-files/output/"
# SPECIFY THE LOCAL PATH OF ONE PAGER DATA EXCEL FILE. Ex: ZambiaTextitOnePagerData.csv
one_pager_textit_data_csv = "ZambiaTextitOnePagerData.csv"
# SPECIFY THE LOCAL PATH OF EXCEL FILE WHICH NEDED TO BE MERGED WITH ONE PAGER DATA EXCEL FILE, BEFORE CLEANED. Ex: Zambia.2016-2017.Growing.rain.storage.maize-buying.xlsx
file_merge_variables = "Zambia.2016-2017.Growing.rain.storage.maize-buying"
# SPECIFY THE LOCAL PATH OF EXCEL FILE WHICH NEDED TO BE CLEANED. THIS IS THE FILE INPUT FOR DATA CLEANING AND OUTPUT OF FILE MERGING.
input_excel_file_name = "Zambia.2016-2017.Growing.rain.storage.maize-buying.final"
# COLUMNS WHICH ARE NEEDED TO MERGE IN ONE FILE
needed_cols = ['name', 'Phone', 'Lat', 'Long']
# YES/NO RESPONSE QUESTIONS
yes_no_response_array = ['rain', 'planting', 'planting complete', 'harvest', 'harvest complete', 'maize purchase', 'maize sold']
# NUMERIC RESPONSE QUESTIONS
numeric_response_array = ['storage', 'expected harvest', 'piece work', '50kg harvest', 'amt maize purchased', 'maize price', 'amt maize sold', 'sales amt', 'piecework']