import pandas as pd
import ConfigParser
import sys
reload(sys)
sys.setdefaultencoding('utf8')

cp = ConfigParser.ConfigParser()
cp.read("config/config.py")

data_files = eval(cp.get("smallholder", "data_files_dir_path"), {}, {})
data_files_output = eval(cp.get("smallholder", "data_files_output_dir_path"), {}, {})
extension = eval(cp.get("smallholder", "extension"), {}, {})

before_clean = eval(cp.get("smallholder", "input_excel_file_name"), {}, {})
final_diff_excel_file = data_files_output + before_clean + ".diff" + extension
after_clean = data_files_output + before_clean + ".cleaned" + extension
before_clean = data_files + before_clean + extension

ques_data = pd.ExcelFile(before_clean)
sheet_names = ques_data.sheet_names

writer = pd.ExcelWriter(final_diff_excel_file, engine='xlsxwriter')

for sheet_name in sheet_names:
	df1 = pd.read_excel(before_clean, sheet_name)
	df2 = pd.read_excel(after_clean, sheet_name)
	
	difference = df1[df1!=df2]
	difference.to_excel(writer, sheet_name=sheet_name)
	
writer.save()