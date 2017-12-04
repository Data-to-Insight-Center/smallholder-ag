# sudo pip install pandas
# sudo pip install xlsxwriter
# sudo pip install xlrd

import pandas as pd
import numpy as np
import ConfigParser
import sys
reload(sys)
sys.setdefaultencoding('utf8')

cp = ConfigParser.ConfigParser()
cp.read("config/config.py")

data_files = eval(cp.get("smallholder", "data_files_dir_path"), {}, {})
extension = eval(cp.get("smallholder", "extension"), {}, {})

one_pager_textit_data_csv = eval(cp.get("smallholder", "one_pager_textit_data_csv"), {}, {})
one_pager_textit_data_csv = data_files + one_pager_textit_data_csv
output_excel_file_path = eval(cp.get("smallholder", "input_excel_file_name"), {}, {})
output_excel_file_path = data_files + output_excel_file_path + extension
file_merge_variables = eval(cp.get("smallholder", "file_merge_variables"), {}, {})
file_merge_variables = data_files + file_merge_variables + extension

# Read in both excel files
one_pager_loc = pd.read_csv(one_pager_textit_data_csv)
needed_cols = eval(cp.get("smallholder", "needed_cols"), {}, {})
one_pager_loc_filter = one_pager_loc[needed_cols]

ques_data = pd.ExcelFile(file_merge_variables)
sheet_names = ques_data.sheet_names  # see all sheet names
writer = pd.ExcelWriter(output_excel_file_path, engine='xlsxwriter')

#Defined merged variables
storage_data = None
maize_storage_data = None
amt_maize_sold_data = None
maize_sold_amt_data = None
amt_maize_purchased_data = None
maize_purchased_data = None

for sheet_name in sheet_names:

	if (sheet_name == "storage"):
		storage_data = pd.read_excel(file_merge_variables, sheet_name, skiprows=1)
		storage_data = storage_data.sort_values(by="name")
	elif (sheet_name == "maize storage"):
		maize_storage_data = pd.read_excel(file_merge_variables, sheet_name, skiprows=1)
		maize_storage_data = maize_storage_data.sort_values(by="name")
	elif (sheet_name == "amt maize sold"):
		amt_maize_sold_data = pd.read_excel(file_merge_variables, sheet_name, skiprows=1)
		amt_maize_sold_data = amt_maize_sold_data.sort_values(by="name")
	elif (sheet_name == "maize sold amt"):
		maize_sold_amt_data = pd.read_excel(file_merge_variables, sheet_name, skiprows=1)
		maize_sold_amt_data = maize_sold_amt_data.sort_values(by="name")
	else:
		every_ques_data = pd.read_excel(file_merge_variables, sheet_name, skiprows=1)
		sort_every_ques_data = every_ques_data.sort_values(by="name")
		every_ques_data_merge = pd.merge(one_pager_loc_filter, sort_every_ques_data, how='outer', on='name')
	
		every_ques_data_merge_final = every_ques_data_merge.replace(np.nan, "na", regex=True)
		every_ques_data_merge_final = every_ques_data_merge_final.replace('-', "na")
		every_ques_data_merge_final.Phone.fillna(every_ques_data_merge_final.phone, inplace=True)
		del every_ques_data_merge_final['phone']
		
		every_ques_data_merge_final.to_excel(writer, sheet_name=sheet_name)
		

if ((maize_storage_data is not None) and (storage_data is not None)):	
	# merge storage and maize storage files 
	merged_storage_data = pd.merge(storage_data, maize_storage_data, how='outer', on=['name','uuid', 'phone'])

	for column in storage_data.columns.values:
		if(column not in ['name', 'uuid', 'phone']):
			storage_data_column = column + "_x"
			maize_storage_data_column = column + "_y"
			merged_storage_data[column] = merged_storage_data[[storage_data_column,maize_storage_data_column]].apply(lambda x : u'{} | {}'.format(x[0],x[1]) if ((np.all(pd.notnull(x[0]) and x[0] != '-')) and (np.all(pd.notnull(x[1]) and x[1] != '-'))) else (u'{}'.format(x[0]) if (np.all(pd.notnull(x[0]) and x[0] != '-')) else (u'{}'.format(x[1]) if (np.all(pd.notnull(x[1]) and x[1] != '-')) else u'na')), axis=1)
			del merged_storage_data[storage_data_column]
			del merged_storage_data[maize_storage_data_column]
		

	storage_merge = pd.merge(one_pager_loc_filter, merged_storage_data, how='outer', on='name')

	storage_merge_final = storage_merge.replace(np.nan, "na", regex=True)
	storage_merge_final.Phone.fillna(storage_merge_final.phone, inplace=True)
	del storage_merge_final['phone']
	
	storage_merge_final.to_excel(writer, sheet_name='storage')

if ((maize_sold_amt_data is not None) and (amt_maize_sold_data is not None)):
	# merge storage and maize storage files 
	merged_amt_maize_sold_data = pd.merge(amt_maize_sold_data, maize_sold_amt_data, how='outer', on=['name','uuid', 'phone'])

	for column in amt_maize_sold_data.columns.values:
		if(column not in ['name', 'uuid', 'phone']):
			amt_maize_sold_data_column = column + "_x"
			merged_amt_maize_sold_data_column = column + "_y"
			merged_amt_maize_sold_data[column] = merged_amt_maize_sold_data[[amt_maize_sold_data_column,merged_amt_maize_sold_data_column]].apply(lambda x : u'{} | {}'.format(x[0],x[1]) if ((np.all(pd.notnull(x[0]) and x[0] != '-')) and (np.all(pd.notnull(x[1]) and x[1] != '-'))) else (u'{}'.format(x[0]) if (np.all(pd.notnull(x[0]) and x[0] != '-')) else (u'{}'.format(x[1]) if (np.all(pd.notnull(x[1]) and x[1] != '-')) else u'na')), axis=1)
			del merged_amt_maize_sold_data[amt_maize_sold_data_column]
			del merged_amt_maize_sold_data[merged_amt_maize_sold_data_column]
		

	amt_maize_sold_merge = pd.merge(one_pager_loc_filter, merged_amt_maize_sold_data, how='outer', on='name')

	amt_maize_sold_merge_final = amt_maize_sold_merge.replace(np.nan, "na", regex=True)
	amt_maize_sold_merge_final.Phone.fillna(amt_maize_sold_merge_final.phone, inplace=True)
	del amt_maize_sold_merge_final['phone']

	amt_maize_sold_merge_final.to_excel(writer, sheet_name='amt maize sold')
	
if ((amt_maize_purchased_data is not None) and (maize_purchased_data is not None)):
	# merge storage and maize storage files 
	merged_amt_maize_purchased_data = pd.merge(amt_maize_purchased_data, maize_purchased_data, how='outer', on=['name','uuid', 'phone'])

	for column in amt_maize_purchased_data.columns.values:
		if(column not in ['name', 'uuid', 'phone']):
			amt_maize_purchased_data_column = column + "_x"
			merged_amt_maize_purchased_data_column = column + "_y"
			merged_amt_maize_purchased_data[column] = merged_amt_maize_sold_data[[amt_maize_purchased_data_column,merged_amt_maize_purchased_data_column]].apply(lambda x : u'{} | {}'.format(x[0],x[1]) if ((np.all(pd.notnull(x[0]) and x[0] != '-')) and (np.all(pd.notnull(x[1]) and x[1] != '-'))) else (u'{}'.format(x[0]) if (np.all(pd.notnull(x[0]) and x[0] != '-')) else (u'{}'.format(x[1]) if (np.all(pd.notnull(x[1]) and x[1] != '-')) else u'na')), axis=1)
			del merged_amt_maize_purchased_data[amt_maize_purchased_data_column]
			del merged_amt_maize_purchased_data[merged_amt_maize_purchased_data_column]

	amt_maize_purchased_merge = pd.merge(one_pager_loc_filter, merged_amt_maize_purchased_data, how='outer', on='name')

	amt_maize_purchased_merge_final = amt_maize_purchased_merge.replace(np.nan, "na", regex=True)
	amt_maize_purchased_merge_final.Phone.fillna(amt_maize_purchased_merge_final.phone, inplace=True)
	del amt_maize_purchased_merge_final['phone']

	amt_maize_purchased_merge_final.to_excel(writer, sheet_name='amt maize purchased')
	
writer.save()