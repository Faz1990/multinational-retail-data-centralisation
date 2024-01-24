import re
pattern = re.compile(r'^[a-zA-Z]{2,3}-[a-zA-Z0-9]{6,9}$')
test_codes = ['HI-F27C7A84', 'WEB-1388012W']
for code in test_codes:
    print(f"{code}: {bool(pattern.match(code))}")
