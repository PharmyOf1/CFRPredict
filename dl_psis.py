import requests, os, re, csv, operator, sys, heapq
from requests_ntlm import HttpNtlmAuth
from bs4 import BeautifulSoup as bs
from bs4 import SoupStrainer
import xlrd
from datetime import datetime, timedelta
from itertools import cycle
from requests.packages.urllib3.exceptions import InsecureRequestWarning

this_path = os.path.dirname(os.path.realpath(__file__))
save_path = os.path.join(this_path, 'psis')
big_guy_final = []
sys.setrecursionlimit(4000)

#2015
def stream_the_links(url_list):
	year = 2015
	for link in url_list:
		gather_links(link, year)
		year += 1

def gather_links(url,year):
	base = 'https://intranet.mdlz.com'
	if year == 2015:
		base = 'https://collaboration.mdlz.com'
	must_contain_list = ['PSI','http']

	requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
	r = requests.get(url, auth=HttpNtlmAuth('KRFT.net\\UNA0464','NWUnwu13'), verify=False)
	x = bs(r.content, "html.parser", parse_only=SoupStrainer('a'))

	all_links = [(a['href']) for a in x if a.has_attr('href')]
	all_links = [decoded for decoded in all_links]
	xl_links = [base+link for link in all_links if str(link).endswith('xlsx') or str(link).endswith('xlsm')]
	xl_links = [link for link in xl_links if all(word in link for word in must_contain_list)]
	#print (xl_links)
	stream_files(xl_links,year)

def stream_files(xl_links,year):
	for link in xl_links:
		fname =  (os.path.basename(link))
		print ('\nWorking on {}'.format(fname))
		match = re.search(r'\d{1,2}-\d{1,2}-\d{1,2}', fname)

		if year == 2015:
			theDate = datetime.strptime(match.group(), '%m-%d-%y').date()
		else:
			theDate = datetime.strptime(match.group(), '%m-%d-%y').date() - timedelta(days=(365.24*4)-1)

		requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
		request = requests.get(link,auth=HttpNtlmAuth('KRFT.net\\UNA0464','NWUnwu13'), stream=True, verify=False)
		with open(os.path.join(save_path,'{}'.format(fname)), 'wb') as f:
			for row in request:
				f.write(row)
		all_data = get_all_data(os.path.join(save_path,'{}'.format(fname)),theDate)

		if all_data is not None:
			big_guy_final.append(all_data)
		os.remove(os.path.join(save_path,'{}'.format(fname)))

def use_local_files(path):
	for x in os.listdir(path):
		if x not in global_loaded_files:
			print ('\n-------{}--------'.format(x))
			all_data = get_all_data(os.path.join(path,x))
			if all_data is not None:
				big_guy_final.append(all_data)

def correct_tab(sheets):
	if 'Output' not in sheets:
		print ('No Output tab')
		return 'Total'
	else:
		print ('Output Tab in File')
		return 'Output'

def extract_date_column(header):
	col = 0
	for a in header:
		if a.ctype == 3:
			first_date_found = datetime(*xlrd.xldate_as_tuple(a.value,0)[0:3]).date()
			print ('{} found in column {}'.format(first_date_found, col))
			return [first_date_found,col]
		col +=1

	# should_be_date = wb.sheet_by_name('Output').cell_value(rowx=0,colx=13)
	# should_be_date = xlrd.xldate_as_tuple(should_be_date,0)
	# should_be_date = datetime(*should_be_date[0:3]).date()

def create_sku_variables(wb,tab,header,date_col):
	data_headers = ['CovDur','DepDmd','ProjOH','TotalDmd','TotSupply']
	col = 0
	for a in header:
		if a.value in ['Aggregate Column', 'Item Code','ItemCode']:
			skus = wb.sheet_by_name(tab).col(col)
			#skus = tab.col(col)
			if (str(skus[2].value)[0]).startswith('0'):
				skus = [sku.value[:14] for sku in skus[1:]]
			else:
				skus = ['00' + str(sku.value)[:12] for sku in skus[1:]] #Sku column values
			sku_strings = list(map(lambda x: str(x[0]) + '_' + str(x[1]),list(zip(skus,cycle(data_headers)))))
			dl = [data.value if data.value not in ['',' '] else 0 for data in wb.sheet_by_name(tab).col(date_col)[1:]]
			#dl_grouped = list(zip(*(iter(dl),)*5)) #Groups these into 5

			for a in dl:
				if isinstance(a,str):
					print (a)

			contains_target = target_exists(header)
			if contains_target:
				target_days = [t.value if t.value not in ['',' '] else 999 for t in wb.sheet_by_name(tab).col(contains_target)[1:]]
			else:
				target_days = [30.0 for _ in dl]

			func1 = lambda x,y,z: float(y)-float(x) if z.endswith('CovDur') else 0
			subtract_target = list(map(func1, target_days,dl,sku_strings))
			below = ([num for num in subtract_target if num<0])
			avg_below = sum(below)/float(len(below))

			total_under_targ = len(below)
			return [sku_strings,dl,total_under_targ, avg_below]
		col+=1
	print ('Nothing returned')
	return []

def filter_data(full_dict):
	final = {a:b for a,b in full_dict.items() if not any([a.endswith('DepDmd'),a.endswith('TotSupply')])}
	final = {sku:val for sku, val in final.items() if sku[:14] in limit_dict(final,1000)}
	final = {sku:value for sku, value in final.items() if any([isinstance(value,float),isinstance(value,int)])}

	final = create_new_variables(final)
	return final

def create_new_variables(final):
	USA = {key:value for key, value in final.items() if key.startswith('004')}

	total_production_us = sum([float(b) for a,b in USA.items() if (a.endswith('TotSupply') and b not in ['', None])])
	total_demand_us = sum([float(b) for a,b in USA.items() if (a.endswith('TotalDmd') and b not in ['', None])])
	total_inv_us = sum([float(b) for a,b in USA.items() if (a.endswith('ProjOH') and b not in ['', None])])

	ITEM_UNDER_2 = len({key for key, value in final.items() if key.endswith('CovDur') and 0<float(value)<2})
	ITEM_UNDER_3 = len({key for key, value in final.items() if key.endswith('CovDur') and 0<float(value)<3})
	ITEM_UNDER_4 = len({key for key, value in final.items() if key.endswith('CovDur') and 0<float(value)<4})

	#final['TOTAL_PRODUCTION_US'] = total_production_us
	final['TOTAL_DEMAND_US'] = total_demand_us
	final['TOTAL_INV_US'] = total_inv_us
	final['ITEMS_UNDER_2'] = ITEM_UNDER_2
	final['ITEMS_UNDER_3'] = ITEM_UNDER_3
	final['ITEMS_UNDER_4'] = ITEM_UNDER_4

	#CovDur - Target

	#Lag 1
	#Lag 2
	#lag 3
	return final

def target_exists(header):
	col = 0
	for txt in header:
		if txt.value == 'Target':
			return col
		col+=1
	return False

def get_all_data(f,theDate=None):
	try:
		wb = xlrd.open_workbook(f,on_demand=True)
		sheets = (wb.sheet_names())
		tab = correct_tab(sheets)
		header = wb.sheet_by_name(tab).row(0)
		theDate, dateCol = extract_date_column(header)
		sku_strings, dl, total_under_targ, avg_below = create_sku_variables(wb,tab,header,dateCol)

		final = filter_data(dict(zip(sku_strings,dl))) #zip the sku_strings and

		final['DATE'] = theDate
		final['UNDER_TARG'] = total_under_targ
		final['AVG_BELOW'] = avg_below

		print ('{} records extracted'.format(len(final)))
		return final

	except Exception as e:
		print ('Error: {}'.format(e))

def write_final_list(final):
	print ('Writing Final')
	fieldnames = sorted(list(set(k for d in final for k in d)))

	with open('test.csv', 'w',newline='') as out_file:
		writer = csv.DictWriter(out_file, fieldnames=fieldnames, dialect='excel')
		writer.writeheader()
		writer.writerows(final)

def limit_dict(d,max_skus=1000):
#=-=-=-=-=-=-=-=Take only top 1000 SKUs-=-=-=-=-=-=-
	all_proj = {key:value for key, value in d.items() if key.endswith('ProjOH') and isinstance(value,float)}
	h = list(sorted(all_proj, key=all_proj.get, reverse=True)[:max_skus])
	top_skus = [sku[:14] for sku in h]
	return top_skus

if __name__ =='__main__':
	#2016
	urls = ['https://collaboration.mdlz.com/sites/productsupplysnackscerealsector/Cost%20Productivity%20%20KPIs/Forms/AllItems.aspx?RootFolder=%2Fsites%2Fproductsupplysnackscerealsector%2FCost%20Productivity%20%20KPIs%2FBiscuit%20Supply%20Planning%20updates%2FPSI%2F2015%20PSI%20Reports#InplviewHashf4431e2c-7702-477b-be30-bfdbf7bb1d21=Paged%3DTRUE-p_SortBehavior%3D0-p_Created%3D20150605%252014%253a56%253a54-p_FileLeafRef%3DPSI%2520Report%2520for%2520%252006%252d05%252d15%252exlsx-p_ID%3D1647-RootFolder%3D%252fsites%252fproductsupplysnackscerealsector%252fCost%2520Productivity%2520%2520KPIs%252fBiscuit%2520Supply%2520Planning%2520updates%252fPSI%252f2015%2520PSI%2520Reports-PageFirstRow%3D31','https://intranet.mdlz.com/sites/rba/workstream_management/Pages/default.aspx?RootFolder=%2Fsites%2Frba%2Fworkstream%5Fmanagement%2FRBA%20Shared%20Docs%2FProduct%20Supply%20and%20Demand%20Planning%2FPSI%2FArchive%2F2016%20Lbs&FolderCTID=0x0120000CA5A2454372D94A902E5C4367173E73&View=%7bFCC2D600-00F0-4E78-9388-5B8337CD5313%7d']
	#stream_the_links(urls)
	global_loaded_files = []
	use_local_files('C:\\Users\\UNA0464\\Desktop\\all_psis')

	#write_final_list(big_guy_final)
