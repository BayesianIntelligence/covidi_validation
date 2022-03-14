import openpyxl, csv

def excelToCsv(inExcelFn, outCsvFn):
	wb = openpyxl.load_workbook(inExcelFn)
	sh = wb.get_active_sheet()
	with open(outCsvFn, 'w') as outCsvFile:
		outCsv = csv.writer(outCsvFile)
		for row in sh.rows:
			outCsv.writerow([cell.value for cell in row])