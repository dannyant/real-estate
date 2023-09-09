from datetime import datetime
date = '20-DEC-19'

print(date)
out = datetime.strptime(date, '%d-%b-%y')
print(out.strftime('%Y%m%d'))
