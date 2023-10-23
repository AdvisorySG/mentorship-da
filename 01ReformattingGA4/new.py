from urllib import parse
import csv
store=[]
processed=[]
with open('data-export.csv', 'r')as basefile:
    for i in range(13):
        heading = next(basefile)
    for row in csv.reader(basefile):
        store+=[row[0]]
for index in range(len(store)):
    store[index]=store[index].replace('%3F','?')
    processed+=[parse.parse_qs(parse.urlsplit(store[index]).query)]
####print(processed)
keystore=[]
uniquekey=[]
valuestore=[]
unwanted=['size','current','fbclid','sort-field','sort-direction','amp;amp;size']
for dictionary in processed:
##    print(dictionary)
    keys = []
    values = []
    for key in dictionary:
        if key in unwanted:
            continue
        keys.append(key)
        values.append(dictionary[key])
        if key not in uniquekey:
            uniquekey.append(key)
    keystore+=[keys]
    valuestore+=[values]
####print(uniquekey)

with open('data-export - Copy.csv', 'w',newline='') as file:
    with open('data-export.csv', 'r')as basefile:
        cursor=csv.writer(file)
        ##hardocding skipping 13 lines
        count=0
        for row in csv.reader(basefile):
            count+=1
            if count<14:
                cursor.writerow(row)
                continue
            for i in valuestore[count-14]:
                if len(i[0])>1:
                    row+=[i]
            cursor.writerow(row)
