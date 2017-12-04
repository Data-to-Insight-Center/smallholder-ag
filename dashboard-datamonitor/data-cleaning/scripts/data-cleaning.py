#pip install inflect
  
import pandas as pd
import numpy as np
import inflect
import ConfigParser
import sys
reload(sys)
sys.setdefaultencoding('utf8')

cp = ConfigParser.ConfigParser()
cp.read("config/config.py")

data_files = eval(cp.get("smallholder", "data_files_dir_path"), {}, {})
data_files_output = eval(cp.get("smallholder", "data_files_output_dir_path"), {}, {})
extension = eval(cp.get("smallholder", "extension"), {}, {})

input_excel_file_path = eval(cp.get("smallholder", "input_excel_file_name"), {}, {})
output_excel_file_path = data_files_output + input_excel_file_path + ".cleaned" + extension
input_excel_file_path = data_files + input_excel_file_path + extension

ques_data = pd.ExcelFile(input_excel_file_path)
sheet_names = ques_data.sheet_names  # see all sheet names
writer = pd.ExcelWriter(output_excel_file_path, engine='xlsxwriter')

yes_no_ques = eval(cp.get("smallholder", "yes_no_response_array"), {}, {})
numeric_ques = eval(cp.get("smallholder", "numeric_response_array"), {}, {})

# text numeric to numeric dictionary map created
numeric_dict = {}
p = inflect.engine()
for i in range(1000):
	key = p.number_to_words(i)
	numeric_dict[key] = i

#print(numeric_dict)
for sheet_name in sheet_names:
	every_ques_data = pd.read_excel(input_excel_file_path, sheet_name)
	
	total_response_column = len(every_ques_data.axes[1])
	
	response_cleanup_others = {}; 
	response_cleanup_nums = {}; 
	response_numeric_clean = {}; 
	response_other_numeric_clean = {}              
	for column in every_ques_data.columns[5:total_response_column]:
		every_ques_data[column] = every_ques_data[column].astype('string').str.lower()
		every_ques_data[column] = every_ques_data[column].astype('string').str.strip()
		response_cleanup_others[column] = {"other-" : ""}
		every_ques_data.loc[every_ques_data[column].str.contains('other-', na=False), column] = every_ques_data[column].str.split("-").str[1]
		every_ques_data.loc[every_ques_data[column].str.contains('|', na=False), column] = every_ques_data[column].str.split("|").str[0]
		every_ques_data.loc[every_ques_data[column].str.contains('kgs', na=False), column] = every_ques_data[column].str.split("x").str[0]
		every_ques_data.loc[every_ques_data[column].str.contains('bags', na=False), column] = every_ques_data[column].str.split("bags").str[0]
		every_ques_data.loc[every_ques_data[column].str.contains('thing|not|dont', na=False), column] = "nothing"
		every_ques_data.loc[every_ques_data[column].str.contains(' ', na=False), column] = every_ques_data[column].str.strip()
		
		if sheet_name in yes_no_ques:
			response_cleanup_nums[column] = {"y | y": "yes", "n | n": "no", "yes | yes": "yes", "no | no": "no", 
			"nothing":"no", "nn" : "no", "it didnt" : "no", "it didn't" : "no", "only sharwas" : "yes", "ti did,nt" : "no", "it isnt rain at this last 7 days" : "no",
			"on" : "no", "only 2 days" : "yes", "is raining" : "yes", "xes" : "yes", "yer" : "yes", "yds" : "yes", "ye": "yes", "yfs":"yes", "yee": "yes", "yers": "yes",
			"it showers today!" : "yes", "yese": "yes", "yss" : "yes", "ys":"yes", "yesn":"yes", "yesddwww":"yes", "yss":"yes", "yesname:zari mobile:0971803564":"yes",
			"yesname:jacob mobile:+260971803564" : "yes" }
		if sheet_name in numeric_ques:
			response_cleanup_nums[column] = {"nothing":0, "yes":"-", "empty":0, "none":0, "nil":0, "nill":0, "nll":0}
			response_numeric_clean[column] = numeric_dict
			response_other_numeric_clean[column] = {"hundred":100, "eight":8, "iv":4, "i00":100, "one hundred and eight":108, "thity":30, 
			"tweenty":20,"three.":3,"th.irty":30}
	
	
	every_ques_data.replace(response_cleanup_others, inplace=True)
	every_ques_data.replace(response_cleanup_nums, inplace=True)
	if sheet_name in numeric_ques:
		every_ques_data.replace(response_numeric_clean, inplace=True)
		every_ques_data.replace(response_other_numeric_clean, inplace=True)
	
	every_ques_data_final = every_ques_data.replace("nan", "na")
	every_ques_data_final = every_ques_data_final.replace('-', "na")
	every_ques_data_final = every_ques_data_final.replace('', "na")
	#print(every_ques_data_final)
	
	for column in every_ques_data_final.columns[5:total_response_column]:
		if sheet_name in yes_no_ques:
			every_ques_data_final.loc[~every_ques_data_final[column].str.match('yes|no', na=False), column] = 'na'
	
	every_ques_data_final.to_excel(writer, sheet_name=sheet_name)
	
writer.save()
