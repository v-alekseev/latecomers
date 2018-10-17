# coding: utf-8

# version 1.0
# # Скрипт для анализа опозданий # python3
# python laters.py "Burykin Nikolay"

# запуск
# В директории 'in' должны быть csv файлы
# c:\Projects\venv\laters\Scripts\activate.bat
# python laters.py "Alekseev Vitaliy"

# Script
# python3 -m venv c:\Projects\venv\laters
# C:\Projects\venv\laters\Scripts\activate.bat
# copy to c:\Projects\venv\laters\src : requirements.txt, laters.py 
# cd  c:\Projects\venv\laters\src
# pip install -r requirements.txt
# python laters.py "Alekseev Vitaliy"

# файлы
# requirements.txt // pip install -r requirements.txt
# laters.py 

# вызов скрипта
# usage: laters.py [-h] file name
# positional arguments:
#   file        input file
#   name        user name
# optional arguments:
#   -h, --help  show this help message and exit

import pandas as pd
import numpy as np

import argparse
import os

from os import listdir
from os.path import isfile, join

import datetime

# # постановка задачи
# Сделать скрипт который открывает файл с информацией о пропусках.
# 
# ## Формат входных данных
# 
# 1. Данные с турникетов <br>
# ['Date','loc','userid','family','name','55','66']
# 
# Date / дата входа / 24.07.18 1:50:55
# loc  / название турникета / Турникет 3 выход/вход
# userid / ID пользователя
# family / Фамилия / Alekseev
# name / Имя / Vitaliy
# 
# 2. имя пользователя
# 3. период запроса.
# 4. согласованое время прихода и ухода
# 
# ## Алгоритм
# Все в рамказ одного пользователя
# Перебираем каждый день. 
#     Для каждого дня получаем список транзакций.
#         X = из списка выбираем саму раннюю с признаком 'вход'
#         Y = и самую позднюю с признаком 'выход'
#         фактическое время нахождения в офисе = Y-X часов минут
#             
#    плановое время нахождение в офисе = {9:пн-чт; 8 пт }
# 
# ## Вывод скрипта
# cvs файл с разделение ';' и заголовками
# Имя пользователя
# ФОРМАТ ВЫВОДА
# Заголовки таблицы.
# 'date'  - дата 
# 'time'  - время проведенное в офисе
# 'name'  - имя сотрудника
# 'dayofweek' - день недели.  'normal' понедельник - четверг
#							  'short' пятница
#							  'dayoff' суббота, воскресенье 
# 'warning'	- Отработано менее положенного или нет. less - меньше, OK - больше или равно
# 'estimate'- Сколько часов положено отработать
# 'delta'  - сколько часов недоработано 

#==============================================================================
# todo
# 6. Разделить все на функции и сделать питоновскую библтотеку.

#==============================================================================


# входные данные

source_path= 'in/'
out_path = 'out/'


days = {
    0: [9, 'normal'],
    1: [9, 'normal'],
    2: [9, 'normal'],
    3: [9, 'normal'],
    4: [8, 'short'],
    5: [9, 'dayoff'],
    6: [9, 'dayoff']
}

warnings = {
True: 'less',
False: 'OK'
} 
def read_file(in_file, name):
    try:
      events = pd.read_csv(in_file,';', header=None, names = ['Date','loc','userid','family','name','55','66'])
    except Exception as e:
      print(e)
      return None
    #подготовим данные.
    # Выделим отдельно даты и время. Date1	Time2	
    yy = np.array([*map(str.split,events['Date'].values)])
    events['Date1'] = yy[:,0]
    events['Time2'] = yy[:,1]
    # Сделаем уникального пользователя user	 (Есть однофамильцы) Если будет полный тезка, будет плохо
    events['user'] = events['family'] + ' ' + events['name']
    # добавим признак вход-выход
    events['action'] = np.array([*map(str.split,events['loc'].values)])[:,2]
    # добавим дату в timedate64
    events['DateNP'] = pd.to_datetime(events['Date'].values, dayfirst = True) 
    # удалим лишние колонки
    events.drop(columns=['55','66', 'family','name','loc'], inplace=True)  

    if args.v:
    	print(events.head(10))
    	print("")
    		
    return events
	

def process_file(source_date, name):



	# константы
	out_colums = ['date','time', 'name', 'dayofweek', 'warning', 'estimate', 'delta']
	outdf = pd.DataFrame(columns=out_colums)  # dataframe с результатом

	# ### сгруппируем по записи по пользователю и потом по дате

	# сгруппируем по записи по пользователю 
	gp = source_date.groupby('user')
	# gp.groups[name] - список ID записей в которых указано имя пользователя
	# source_date.iloc[gp.groups[name]] - записи только по пользователю name
	#.groupby('Date1') сгруппируем записи по дате
	try:
	   g2= source_date.iloc[gp.groups[name]].groupby('Date1')
	except KeyError as e:
	   print ('User ' + name + ' not found')
	   return None  
	   
	for gl2 in g2.groups:    
		# получаем список входов и выходов для каждой даты и считаем самый позднее время - самое раннее
		date = source_date.iloc[g2.groups[gl2]]
		if args.v:
		   print(date)
		   
		aa = pd.to_datetime(date['DateNP'].values.max(), dayfirst = True)-pd.to_datetime(date['DateNP'].values.min(), dayfirst = True)
		
		#проверим что за день недели 
		curdate = pd.to_datetime(date['DateNP'].values.max(), dayfirst = True)
		try:
		 dayofweek = days[curdate.dayofweek][1]
		except KeyError as e:
		 dayofweek = 'error'

		# проверим выработал или нет свое время сотрудник
		warning = warnings[aa.components.hours < days[curdate.dayofweek][0]]
		
		# рассчитаем сколько не доработал сотрудник
		if  pd.Timedelta(days[curdate.dayofweek][0],'h') >  aa:
			jj = pd.Timedelta(days[curdate.dayofweek][0],'h') - aa
			delta = '-' + '{:02}:{:02}:{:02}'.format(int(jj.components.hours), int(jj.components.minutes), int(jj.components.seconds))	
		else:
			jj = aa - pd.Timedelta(days[curdate.dayofweek][0],'h')
			delta = '+' + '{:02}:{:02}:{:02}'.format(int(jj.components.hours), int(jj.components.minutes), int(jj.components.seconds))	
			 
		if args.v:
		   print(jj.components)
		   
		
		# Подготавливаем вывод данных
		retstring = '{:02}:{:02}:{:02}'.format(int(aa.components.hours), int(aa.components.minutes), int(aa.components.seconds))
		row = [date['Date1'].values[0] ,retstring , name, dayofweek, warning, str(days[curdate.dayofweek][0]) + ' hours',delta]
		print (row[0],row[1], dayofweek, delta)
		outdf = outdf.append(pd.DataFrame([row], columns=out_colums), ignore_index=True)

	return outdf


def export_to_file(data, filename):
	# сохраним в csv
	data.to_csv(filename,index_label='index')

	full_out_path = os.path.join(os.getcwd(),filename)
	print ('Result in file', full_out_path)
	print()

	
# ###################################################################
# Start

# parsing command line arguments
parser = argparse.ArgumentParser()
#parser.add_argument("files", help="input files [1.csv, 2csv]")
parser.add_argument("name", help="user name [Ivanov Petr] ")
parser.add_argument("-v", help="show trace message", action="store_true")
args = parser.parse_args()

if args.v:
    print("Debug loging is turned on")

name = args.name # user name
#files = args.files # source file name

# читаем файлы из директории source_path
onlyfiles = [f for f in listdir(source_path) if isfile(join(source_path, f))]
files = ','.join(onlyfiles)
if args.v:
    print('I found next files:' + files)
    print("")

	
total_df = 	pd.DataFrame() 
# ## цикл по файлам 
for file in [x.strip() for x in files.split(',')]:


	if not os.path.exists(out_path):
		os.makedirs(out_path)
    # KanadinSergej_2018.09.09-15_out.csv
	out_file = out_path + name.replace(' ','') + '_' +os.path.splitext(file)[0] + '_out.csv'

	in_file = source_path + file
	print ('Processing file', in_file, 'with user', name)

	# read file
	source_date =read_file(in_file, name)
	if source_date is None:
	  print('Error read file '+ in_file)
	  exit()

	# process data
	result_data = process_file(source_date, name)
	
	total_df = total_df.append(result_data, ignore_index=True)

	# write results
#	if result_data is not None:
#		export_to_file(result_data, out_file)
	# ## конец цикл по файлам

one_out_file = os.path.abspath(out_path + name.replace(' ','') + '_' + str(datetime.datetime.now().date()) + '_out.csv')

if args.v:
	print(total_df)

if total_df is not None:
	export_to_file(total_df, one_out_file)
	

#"Hramov Ivan"
#"Kanadin Sergej"
#"Spirin Dmitry"
#Alekseev Vitaliy
#Bogarov	Andrey
#Osipov	Dmitry
#Nekoshnov Georgy
# Kuznetsova Marina


