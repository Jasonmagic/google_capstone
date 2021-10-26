from Recode_LocalDB import financial_Data

ticker = input('Enter a Ticker code :')
financialYear = input('Enter reporting year :')
reportCode = input('Enter report code (10-K or 10-Q) :')
data = financial_Data(ticker, financialYear, reportCode)
financial_Data.insert_database(data)