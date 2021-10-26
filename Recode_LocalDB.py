
import sys
import time
import requests
import pyodbc
import datetime
import pandas as pd
from bs4 import BeautifulSoup as bs
from IPython.display import display
from lxml import etree as ET

class financial_Data():

    def __init__(self, ticker, financialYear, reportCode):
        self.ticker = ticker.upper()
        self.owner = 'JASL'
        self.projection = False
        self.financialYear = int(financialYear)
        self.uploadYear = self.financialYear + 1 if reportCode == "10-K" else self.financialYear
        self.reportCode = reportCode
        self.cursor_object = financial_Data.init_SQL()
        self.cik, self.companyName = financial_Data.print_Company(self)
        self.xmlDoc, self.preDoc, self.calDoc, self.labDoc, self.period_ends = financial_Data.report_URL(self)
        financial_Data.init_error_Handling(self)
    
    def init_error_Handling(self):
        if (self.reportCode != '10-Q' and self.reportCode != '10-K'):
            sys.exit('ERROR: Report type not supported')

    def fair_access(link):
        time.sleep(1)
        detector = 403
        while detector == 403:
            co_webcontent = requests.get(link, headers={'User-Agent': 'Company Name myname@company.com'}, stream=True)
            detector = co_webcontent.status_code
        return co_webcontent

    def print_Company(self):
        com_link = "https://www.sec.gov/cgi-bin/browse-edgar?ticker={}&action=getcompany".format(self.ticker)
        co_info = bs(financial_Data.fair_access(com_link).content, 'html.parser').find('span', class_ = 'companyName')
        cik_info = bs(financial_Data.fair_access(com_link).content, 'html.parser').find('a', id = 'documentsbutton')
        companyName = co_info.text.split(' CIK#: ')[0].split('/')[0]
        cik = cik_info.attrs['href'].split('/')[4]
        print(cik,companyName)
        return cik, companyName

    def init_SQL():
        # Define the Components of the Connection String.
        DRIVER = '{ODBC Driver 17 for SQL Server}'
        SERVER_NAME = "JASONPC"
        DATABASE_NAME = "SEC_v2021_10"

        CONNECTION_STRING = """
        Driver={driver};
        Server={server};
        Database={database};
        Trusted_Connection=yes;
        """.format(
            driver=DRIVER,
            server=SERVER_NAME,
            database=DATABASE_NAME
        )

        # Create a connection object.
        connection_object: pyodbc.Connection = pyodbc.connect(CONNECTION_STRING, autocommit = True)
        # Create a Cursor Object, using the connection.
        cursor_object: pyodbc.Cursor = connection_object.cursor()
        
        return cursor_object

    def make_url(base_url , comp):
        url = base_url
        for r in comp:
            url = '{}/{}'.format(url, r)
        return url

    def communications(stage, *args):
        print('-'*100)
        if stage == 1:
            print('Building the URL for Financial Year: {}'.format(args[0]))
        elif stage == 2:
            print('Pulling url for Quarter: {}'.format(args[0]))
            print("URL Link: " + args[1])


    def search_masterdoc(self):
        base_url = r"https://www.sec.gov/Archives/edgar/full-index"
        year_url = financial_Data.make_url(base_url, [self.uploadYear, 'index.json'])
        
        financial_Data.communications(1, self.financialYear)

        year_folder = financial_Data.fair_access(year_url).json()
        qtr_count = []
        for qtr in year_folder['directory']['item'][0:]:
            qtr_url = financial_Data.make_url(base_url, [self.uploadYear, qtr['name'], 'index.json'])
            financial_Data.communications(2, qtr['name'], qtr_url)
            qtr_count.append(qtr['name'])
        
        master_data = []
    
        for index, qtr in enumerate(qtr_count):
            if (self.reportCode == '10-K' and len(master_data) > 0) or \
                ((self.reportCode == '10-Q' and len(master_data) > index) or \
                    len(master_data) == 3):
                break
            
            else:
                file_url = financial_Data.make_url(base_url, [self.uploadYear, qtr, 'master.idx'])
                file_content = financial_Data.fair_access(file_url)
                data = file_content.content.decode("latin-1").split('Date Filed|Filename')

                for url in data:
                    new_item = url.replace('\n','|').split('|')

                    for count, row in enumerate(new_item):

                        # when you find the text file.
                        if '.txt' in row and self.cik == new_item[(count - 4)] and self.reportCode == new_item[(count - 2)]:

                            # grab the values that belong to that row. It's 4 values before and one after.
                            mini_list = new_item[(count - 4): count + 1]
                            mini_list[4] = "https://www.sec.gov/Archives/" + mini_list[4]
                            master_data.append(mini_list)   
        return master_data

    def report_URL(self):
        master_data = financial_Data.search_masterdoc(self)

        self.period_ends = []
        documents_url = []
        date_storage = []
        self.xmlDoc = []
        self.preDoc = []
        self.calDoc = []
        self.labDoc = []
        
        # loop through each document in the master list.
        for document in master_data:

            # create a dictionary for each document in the master list
            document_dict = {}
            document_dict['date'] = datetime.datetime.strptime(document[3],"%Y-%m-%d")
            document_dict['file_url'] = document[4]
        
            txt_content = financial_Data.fair_access(document[4])
            data = txt_content.content.decode("latin-1").split('CONFORMED PERIOD OF REPORT:')[1].split('FILED AS OF DATE:')

            period_end = data[0].strip()
            self.period_ends.append(period_end)
            
            date_storage.append(document_dict['date'])
            archive_url = document_dict['file_url'].replace('.txt','').replace('-','')
            documents_url = financial_Data.make_url(archive_url,['index.json'])
            doc_content = financial_Data.fair_access(documents_url).json()
            
            for file in doc_content['directory']['item']:
                
                if '.xml' in file['name'] and not 'cal' in file['name'] and not 'pre' in file['name'] and not 'lab' in file['name'] and not 'def' in file['name'] and not 'FilingSummary' in file['name']:
                    xml_link = "https://www.sec.gov" + doc_content['directory']['name'] + "/" + file['name']
                    self.xmlDoc.append(xml_link)
                elif 'pre.xml' in file['name']:
                    pre_link = "https://www.sec.gov" + doc_content['directory']['name'] + "/" + file['name']
                    self.preDoc.append(pre_link)
                elif 'cal.xml' in file['name']:
                    cal_link = "https://www.sec.gov" + doc_content['directory']['name'] + "/" + file['name']
                    self.calDoc.append(cal_link)
                elif 'lab.xml' in file['name']:
                    lab_link = "https://www.sec.gov" + doc_content['directory']['name'] + "/" + file['name']
                    self.labDoc.append(lab_link)
        
        if len(self.xmlDoc) == 0:
            sys.exit('Requested filing does not exist.')

        return self.xmlDoc, self.preDoc, self.calDoc, self.labDoc, self.period_ends

    def check_database(self):               
        report_year = self.period_ends[0][0:4]
        DATA_BASE = ['XMLDATA', 'PREDATA', 'LABDATA', 'CALDATA']
        
        # Define our Query to create the table.
        create_table_XML = """
        IF Object_ID('XMLDATA') IS NULL
        CREATE TABLE [SEC_v2021_10].[dbo].[XMLDATA]
        (
            [GAAP_Code] NVARCHAR(MAX) NOT NULL,
            [Amount] FLOAT NOT NULL,
            [Year] INT NOT NULL,
            [CIK] INT NOT NULL,
            [Company Name] NVARCHAR(MAX) NOT NULL,
            [Report Code] NVARCHAR(MAX) NOT NULL,
            [Ticker] NVARCHAR(MAX) NOT NULL,
            [Report Date] DATE NOT NULL,
            [Quarter] INT NOT NULL,
            [Segment] NVARCHAR(MAX) NOT NULL,
            [UniqueID] NVARCHAR(MAX) NOT NULL,
            [Coverage] NVARCHAR(MAX) NOT NULL,
            [Projection] BIT NOT NULL, 
            [Owner] NVARCHAR(MAX) NOT NULL       
        )
        """
        # 'Statement ID', 'Parent_GAAP', 'GAAP_Code', 'Order', 'Year', 'CIK', 'Company Name', 'Report Code', 'Ticker', 'Quarter'
        create_table_PRE = """
        IF Object_ID('PREDATA') IS NULL
        CREATE TABLE [SEC_v2021_10].[dbo].[PREDATA]
        (
            [Statement ID] NVARCHAR(MAX) NOT NULL,
            [Parent_GAAP] NVARCHAR(MAX) NOT NULL,
            [GAAP_Code] NVARCHAR(MAX) NOT NULL,
            [Order] FLOAT NOT NULL,
            [Year] INT NOT NULL,
            [CIK] INT NOT NULL,
            [Company Name] NVARCHAR(MAX) NOT NULL,
            [Report Code] NVARCHAR(MAX) NOT NULL,
            [Ticker] NVARCHAR(MAX) NOT NULL,
            [Quarter] INT NOT NULL,
            [Owner] NVARCHAR(MAX) NOT NULL
        )
        """
        
        create_table_CAL = """
        IF Object_ID('CALDATA') IS NULL
        CREATE TABLE [SEC_v2021_10].[dbo].[CALDATA]
        (
            [Statement ID] NVARCHAR(MAX) NOT NULL,
            [Parent_GAAP] NVARCHAR(MAX) NOT NULL,
            [GAAP_Code] NVARCHAR(MAX) NOT NULL,
            [Order] FLOAT NOT NULL,
            [Weight] INT NOT NULL,
            [Year] INT NOT NULL,
            [CIK] INT NOT NULL,
            [Company Name] NVARCHAR(MAX) NOT NULL,
            [Report Code] NVARCHAR(MAX) NOT NULL,
            [Ticker] NVARCHAR(MAX) NOT NULL,
            [Quarter] INT NOT NULL,
            [Owner] NVARCHAR(MAX) NOT NULL
        )
        """

        create_table_LAB = """
        IF Object_ID('LABDATA') IS NULL
        CREATE TABLE [SEC_v2021_10].[dbo].[LABDATA]
        (
            [LABEL TYPE] NVARCHAR(MAX) NOT NULL,
            [GAAP_Code] NVARCHAR(MAX) NOT NULL,
            [LABEL] NVARCHAR(MAX) NULL,
            [Year] INT NOT NULL,
            [CIK] INT NOT NULL,
            [Company Name] NVARCHAR(MAX) NOT NULL,
            [Report Code] NVARCHAR(MAX) NOT NULL,
            [Ticker] NVARCHAR(MAX) NOT NULL,
            [Quarter] INT NOT NULL,
            [Owner] NVARCHAR(MAX) NOT NULL
        )
        """
        
        
        # Create the Table.
        self.cursor_object.execute(create_table_XML)
        self.cursor_object.execute(create_table_PRE)
        self.cursor_object.execute(create_table_CAL)
        self.cursor_object.execute(create_table_LAB)
        
        # Define the Insert Query.
        sql_extract = """SELECT * FROM [SEC_v2021_10].[dbo].[XMLDATA] WHERE [Year] = '""" + report_year + \
            """' AND [CIK] = '""" + self.cik + """' AND [Report Code] = '""" + self.reportCode + "'"
        
        # Execute it.
        data = self.cursor_object.execute(sql_extract).fetchall()

        if len(data) > 0:
            Action = input('Dataset exists. Need update? (Y/N)')
            # Action = 'Y'
            if Action == 'Y':
                
                for data_set in DATA_BASE:
                    
                    sql_del = """DELETE FROM [SEC_v2021_10].[dbo].[""" + data_set + """] WHERE [Year]= '""" + report_year + \
                        """' AND [CIK] = '""" + self.cik + """' AND [Report Code] = '""" + self.reportCode + "'"

                    self.cursor_object.execute(sql_del)
                
            else: 
                sys.exit('System exit')

    def standardised_Data(self, *args):

        index = args[1]
        filing = args[2]
        
        if self.reportCode == '10-K':
            reportQTR = 4
            qtr_date_formatted = self.period_ends[index][0:4] + '-' + self.period_ends[index][4:6] + '-' + self.period_ends[index][6:8]
                
        elif self.reportCode == '10-Q':
            if int(self.period_ends[index][4:6]) >=1 and int(self.period_ends[index][4:6]) <=3:
                reportQTR = 1
                qtr_date_formatted = self.period_ends[index][0:4] + '-' + self.period_ends[index][4:6] + '-' + self.period_ends[index][6:8]

            elif int(self.period_ends[index][4:6]) >=4 and int(self.period_ends[index][4:6]) <=6:
                reportQTR = 2
                qtr_date_formatted = self.period_ends[index][0:4] + '-' + self.period_ends[index][4:6] + '-' + self.period_ends[index][6:8]

            elif int(self.period_ends[index][4:6]) >=7 and int(self.period_ends[index][4:6]) <=9:
                reportQTR = 3
                qtr_date_formatted = self.period_ends[index][0:4] + '-' + self.period_ends[index][4:6] + '-' + self.period_ends[index][6:8]

            elif int(self.period_ends[index][4:6]) >=10 and int(self.period_ends[index][4:6]) <=12:
                reportQTR = 4
                qtr_date_formatted = self.period_ends[index][0:4] + '-' + self.period_ends[index][4:6] + '-' + self.period_ends[index][6:8]
        
        filing_content = financial_Data.fair_access(filing)
        tree = ET.XML(filing_content.content)

        return tree, reportQTR, qtr_date_formatted

    def xml_doc(self):
        
        data_content = self.xmlDoc
        
        Lib_lookupid = {}
        Library = {}
        
        for index, parsingLink in enumerate(data_content):
            
            STD_Data = financial_Data.standardised_Data(self, self.period_ends, index, parsingLink)
            tree = STD_Data[0]
            reportQTR = STD_Data[1]
            qtr_date_formatted = STD_Data[2]
            reportYear = self.period_ends[index][0:4]

            elements_instant = tree.findall('.//{http://www.xbrl.org/2003/instance}instant')
            elements_enddate = tree.findall('.//{http://www.xbrl.org/2003/instance}endDate')
            
            Lib_lookupid[reportQTR] = {}
            Library[reportQTR] = {}
            
            # Building SEGMENT Library - 1
            for ele in elements_enddate:
                dimension = ''
                
                if ele.text == qtr_date_formatted:
                    cal_1 = datetime.date.fromisoformat(ele.text)
                    cal_2 = datetime.date.fromisoformat(ele.getprevious().text)
                    
                    if (cal_1 - cal_2).days < 100 and reportQTR != 1:
                        RPT_period = 'QTD' 
                    elif (cal_1 - cal_2).days > 400:
                        RPT_period = 'SPECIFIC_PERIOD_RANGE' 
                    else:
                        RPT_period = 'YTD'
                    
                    segment_id = ele.getparent().getparent().attrib['id']
                    
                    if ele.getparent().getprevious().find('.//{http://www.xbrl.org/2003/instance}segment') == None:
                        dimension = 'MAIN_SEGMENT'
                        Lib_lookupid[reportQTR][segment_id] = {}
                        Lib_lookupid[reportQTR][segment_id]['SEGMENT'] = dimension
                        Lib_lookupid[reportQTR][segment_id]['COVERAGE'] = RPT_period

                    else:
                        for i in ele.getparent().getprevious().findall('.//{http://xbrl.org/2006/xbrldi}explicitMember'):
                            dimension += '_' + i.attrib['dimension'] + '_' + i.text

                        Lib_lookupid[reportQTR][segment_id] = {}
                        Lib_lookupid[reportQTR][segment_id]['SEGMENT'] = dimension
                        Lib_lookupid[reportQTR][segment_id]['COVERAGE'] = RPT_period

            # Building SEGMENT Library - 2
            for ele in elements_instant:
                dimension = ''
                
                if ele.text == qtr_date_formatted:
                    RPT_period = 'YTD'
                    segment_id = ele.getparent().getparent().attrib['id']

                    if ele.getparent().getprevious().find('.//{http://www.xbrl.org/2003/instance}segment') == None:
                        dimension = 'MAIN_SEGMENT'
                        Lib_lookupid[reportQTR][segment_id] = {}
                        Lib_lookupid[reportQTR][segment_id]['SEGMENT'] = dimension
                        Lib_lookupid[reportQTR][segment_id]['COVERAGE'] = RPT_period

                        
                    else:
                        for i in ele.getparent().getprevious().findall('.//{http://xbrl.org/2006/xbrldi}explicitMember'):
                            dimension += '_' + i.attrib['dimension'] + '_' + i.text

                        Lib_lookupid[reportQTR][segment_id] = {}
                        Lib_lookupid[reportQTR][segment_id]['SEGMENT'] = dimension
                        Lib_lookupid[reportQTR][segment_id]['COVERAGE'] = RPT_period
            
            # Building GAAP Library
            for ele in tree.iter():
                try:
                    con_ref = ele.attrib['contextRef']
                    decis = ele.attrib['decimals']
                    unique_id = ele.attrib['id']
                    gaap_id = ele.tag.split('}')[1]
                    try:
                        value = float(ele.text)
                    except:
                        value = 0
                    
                    if decis == 'INF' or int(decis) >= -6:
                        Library[reportQTR][unique_id] = {}
                        Library[reportQTR][unique_id]['SEGMENT_ID'] = con_ref
                        Library[reportQTR][unique_id]['GAAP_ID'] = gaap_id
                        Library[reportQTR][unique_id]['VALUE'] = value
                    
                except:
                    continue
            
            # Consolidate GAAP and SEGMENT Library
            for qtr in Library.values():
                for uniqueID in qtr.values():
                    segmentID = uniqueID['SEGMENT_ID']
                    for seg_id in Lib_lookupid[reportQTR].keys():
                        if segmentID == seg_id:
                            uniqueID['DIMENSION'] = Lib_lookupid[reportQTR][seg_id]['SEGMENT']
                            uniqueID['COVERAGE'] = Lib_lookupid[reportQTR][seg_id]['COVERAGE']
                            uniqueID['REPORT_QTR'] = reportQTR
                            uniqueID['REPORT_YR'] = reportYear
                            uniqueID['UPLOADING_DATE'] = self.period_ends[index]
        
        return Library

    def XML_Decoration(self):
        data_content = financial_Data.xml_doc(self)        
        data_pack = []
        
        for qtr in data_content.values():
            for uni_ID in qtr.values():

                if len(uni_ID) == 8:
                    dataset = [uni_ID['GAAP_ID'], uni_ID['VALUE'], uni_ID['REPORT_YR'], \
                            self.cik, self.companyName, self.reportCode, self.ticker, uni_ID['UPLOADING_DATE'], \
                            uni_ID['REPORT_QTR'], uni_ID['DIMENSION'], uni_ID['SEGMENT_ID'], \
                                uni_ID['COVERAGE'], self.projection, self.owner]
                    data_pack.append(dataset)
        return data_pack

    def XMLData_frame(self):
        
        data_content = financial_Data.XML_Decoration(self)

        # Put the data in a DataFrame
        income_df = pd.DataFrame(data_content)

        # Define the Index column, rename it, and we need to make sure to drop the old column once we reindex.
        income_df.index.name = 'Index'
        income_df.columns = ['GAAP_Code', 'Amount', 'Year', 'CIK', 'Company Name', 'Report Code', 'Ticker', 'Report Date', 'Quarter', 'Segment', 'Unique_ID', 'Coverage', 'Projection', 'Owner']

        # show the df
        display(income_df)

        # drop the data in a CSV file if need be.
        income_df.to_csv('Test.csv')

        # Delete dataframe
        income_df = pd.DataFrame()

        return income_df
    
    def pre_doc(self):
        
        data_content = self.preDoc

        Pre_dic = {}

        for index, parsingLink in enumerate(data_content):
            
            STD_Data = financial_Data.standardised_Data(self, self.period_ends, index, parsingLink)
            tree = STD_Data[0]
            reportQTR = STD_Data[1]
            reportYear = self.period_ends[index][0:4]

            extract = tree.findall('.//{http://www.xbrl.org/2003/linkbase}presentationLink')
            
            Pre_dic[reportQTR] = {}
            
            for element in extract:
                if len(element.findall('.//{http://www.xbrl.org/2003/linkbase}presentationArc')) > 0 :
                    statement_ID = element.attrib['{http://www.w3.org/1999/xlink}role'].split('role/')[1]
                    Pre_dic[reportQTR][statement_ID] = []
                else:
                    continue
                    
                for sub_ele in element.findall('.//{http://www.xbrl.org/2003/linkbase}presentationArc'):
                    acc_parent = sub_ele.attrib['{http://www.w3.org/1999/xlink}from'].split('_')[2]
                    acc_name = sub_ele.attrib['{http://www.w3.org/1999/xlink}to'].split('_')[2]
                    acc_order = sub_ele.attrib['order'] if len(sub_ele.attrib['order']) > 0 else None
                    data = [statement_ID, acc_parent, acc_name, acc_order, reportYear, self.cik, self.companyName, self.reportCode, self.ticker, reportQTR, self.owner]
                    Pre_dic[reportQTR][statement_ID].append(data)

        return Pre_dic

    def PRE_Decoration(self): 
        data_content = financial_Data.pre_doc(self)
        data_pack = []
        
        for qtr in data_content:
            for statement_ID in data_content[qtr]:
                data_pack.extend(data_content[qtr][statement_ID])

        return data_pack

    def PREData_frame(self):

        data_content = financial_Data.PRE_Decoration(self)
        
        # Put the data in a DataFrame
        income_df = pd.DataFrame(data_content)

        # Define the Index column, rename it, and we need to make sure to drop the old column once we reindex.
        income_df.index.name = 'Index'
        income_df.columns = ['Statement ID', 'Parent_GAAP', 'GAAP_Code', 'Order', 'Year', 'CIK', 'Company Name', 'Report Code', 'Ticker', 'Quarter', 'Owner']

        # show the df
        display(income_df)

        # drop the data in a CSV file if need be.
        income_df.to_csv('Test.csv')

        # Delete dataframe
        income_df = pd.DataFrame()

        return income_df

    def cal_doc(self):
        
        data_content = self.calDoc

        cal_dic = {}

        for index, parsingLink in enumerate(data_content):
            
            STD_Data = financial_Data.standardised_Data(self, self.period_ends, index, parsingLink)
            tree = STD_Data[0]
            reportQTR = STD_Data[1]
            reportYear = self.period_ends[index][0:4]

            extract = tree.findall('.//{http://www.xbrl.org/2003/linkbase}calculationLink')
            
            cal_dic[reportQTR] = {}
            
            for ele in extract:
                if len(ele.findall('.//{http://www.xbrl.org/2003/linkbase}calculationArc')) > 0:
                    statement_ID = ele.attrib['{http://www.w3.org/1999/xlink}role'].split('role/')[1]  
                    cal_dic[reportQTR][statement_ID] = []
                else:
                    continue
                
                for sub_ele in ele.findall('.//{http://www.xbrl.org/2003/linkbase}calculationArc'):
                    acc_parent = sub_ele.attrib['{http://www.w3.org/1999/xlink}from'].split('_')[2]
                    acc_name = sub_ele.attrib['{http://www.w3.org/1999/xlink}to'].split('_')[2]
                    acc_order = float(sub_ele.attrib['order'])
                    acc_weight = float(sub_ele.attrib['weight'])
                    data = [statement_ID, acc_parent, acc_name, acc_order, acc_weight, reportYear, self.cik, self.companyName, \
                        self.reportCode, self.ticker, reportQTR, self.owner]
                    cal_dic[reportQTR][statement_ID].append(data)

        return cal_dic

    def CAL_Decoration(self): 
        
        data_content = financial_Data.cal_doc(self)
        data_pack = []
        
        for qtr in data_content:
            for statement_ID in data_content[qtr]:
                data_pack.extend(data_content[qtr][statement_ID])

        return data_pack

    def CALData_frame(self):
        
        data_content = financial_Data.CAL_Decoration(self)

        # Put the data in a DataFrame
        income_df = pd.DataFrame(data_content)

        # Define the Index column, rename it, and we need to make sure to drop the old column once we reindex.
        income_df.index.name = 'Index'
        income_df.columns = ['Statement ID', 'Parent_GAAP', 'GAAP_Code', 'Order', 'Weight', 'Year', 'CIK', 'Company Name', 'Report Code', 'Ticker', 'Quarter', 'Owner']

        # show the df
        display(income_df)

        # drop the data in a CSV file if need be.
        income_df.to_csv('Test.csv')

        # Delete dataframe
        income_df = pd.DataFrame()

        return income_df

    def lab_doc(self):
        
        lab_dic = {}

        data_content = self.labDoc

        for index, parsingLink in enumerate(data_content):
            
            STD_Data = financial_Data.standardised_Data(self, self.period_ends, index, parsingLink)
            tree = STD_Data[0]
            reportQTR = STD_Data[1]
            reportYear = self.period_ends[index][0:4]

            extract = tree.findall('.//{http://www.xbrl.org/2003/linkbase}labelLink')
            
            lab_dic[reportQTR] = []
            
            for ele in extract:
                for item in ele.findall('.//{http://www.xbrl.org/2003/linkbase}label'):
                    label_type = item.attrib['{http://www.w3.org/1999/xlink}role'].split('role/')[1]
                    acc_name = item.attrib['{http://www.w3.org/1999/xlink}label'].split('_')[2]
                    label = item.text
                    
                    data = [label_type, acc_name, label, reportYear, self.cik, self.companyName, self.reportCode, \
                        self.ticker, reportQTR, self.owner]
                    lab_dic[reportQTR].append(data)

        return lab_dic

    def LAB_Decoration(self): 
        
        data_content = financial_Data.lab_doc(self)
        data_pack = []
        
        for qtr in data_content.values():
            for label in qtr:
                data_pack.append(label)

        return data_pack

    def LABData_frame(self):
        
        data_content = financial_Data.LAB_Decoration(self)

        # Put the data in a DataFrame
        income_df = pd.DataFrame(data_content)

        # Define the Index column, rename it, and we need to make sure to drop the old column once we reindex.
        income_df.index.name = 'Index'
        income_df.columns = ['LABEL TYPE', 'GAAP_Code', 'LABEL', 'Year', 'CIK', 'Company Name', 'Report Code', 'Ticker', 'Quarter', 'Owner']

        # show the df
        display(income_df)

        # drop the data in a CSV file if need be.
        income_df.to_csv('Test.csv')

        # Delete dataframe
        income_df = pd.DataFrame()

        return income_df

    def insert_database(self):
        
        financial_Data.check_database(self)
        xmldata_content = financial_Data.XML_Decoration(self)
        predata_content = financial_Data.PRE_Decoration(self)
        caldata_content = financial_Data.CAL_Decoration(self)
        labdata_content = financial_Data.LAB_Decoration(self)

        # Define the Insert Query.
        xmlsql_insert = """
        INSERT INTO [SEC_v2021_10].[dbo].[XMLDATA]
        (
            [GAAP_Code], [Amount], [Year], [CIK], [Company Name], [Report Code], [Ticker], [Report Date], [Quarter], [Segment], [UniqueID], [Coverage], [Projection], [Owner]
        )
        VALUES
        (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )
        """
        presql_insert = """
        INSERT INTO [SEC_v2021_10].[dbo].[PREDATA]
        (
            [Statement ID], [Parent_GAAP], [GAAP_Code], [Order], [Year], [CIK], [Company Name], [Report Code], [Ticker], [Quarter], [Owner]
        )
        VALUES
        (
        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )"""
        
        calsql_insert = """
        INSERT INTO [SEC_v2021_10].[dbo].[CALDATA]
        (
            [Statement ID], [Parent_GAAP], [GAAP_Code], [Order], [Weight], [Year], [CIK], [Company Name], [Report Code], [Ticker], [Quarter], [Owner]
        )
        VALUES
        (
        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )"""
        
        labsql_insert = """
        INSERT INTO [SEC_v2021_10].[dbo].[LABDATA]
        (
            [LABEL TYPE], [GAAP_Code], [LABEL], [Year], [CIK], [Company Name], [Report Code], [Ticker], [Quarter], [Owner]
        )
        VALUES
        (
        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )"""
        
        # Execute it.
        self.cursor_object.executemany(xmlsql_insert, xmldata_content)
        self.cursor_object.executemany(presql_insert, predata_content)
        self.cursor_object.executemany(calsql_insert, caldata_content)
        self.cursor_object.executemany(labsql_insert, labdata_content)

        print("-"*100 + "\nProgram Finished\n" + "-"*100)