import json
import gzip
def main():
    jsonArray = []

    for i in range(1, 11):
        temp = {'id': i, 'image': str(i)+'.png', 'textPath': str(i)+'.txt', 'textPathModfied': '2019-9-9 12:12:12',
    'summaryPath': str(i)+'summary.txt', 'summaryPathModfied': '2019-9-9 12:12:20', 'processTimeSecond': i+2}
        jsonArray.append(temp)

    for i in range(3, 11):
        temp = {'id': i, 'image': str(i)+'.png', 'textPath': str(i)+'.txt', 'textPathModfied': '2019-9-9 12:12:12',
    'summaryPath': str(i)+'summary.txt', 'summaryPathModfied': '2019-9-9 12:12:20', 'processTimeSecond': i+1}
        jsonArray.append(temp)

    firstJson = json.dumps(jsonArray)

    # with open('./jsonfile.json', 'w') as outfile:
    #     file = json.dump(jsonArray, outfile, indent=4)
    # print(firstJson)

    try:
        data = json.loads(firstJson)
        print ("Is valid json? true") 
    except ValueError as e: 
        print ("Is valid json? false") 

    with open('usage.json', 'r') as infile:
        jsonObj = json.load(infile)
        jsonObj.append({'id': 99, 'image': str(99)+'.png', 'textPath': str(99)+'.txt', 'textPathModfied': '2019-9-9 12:12:12',
    'summaryPath': str(99)+'summary.txt', 'summaryPathModfied': '2019-9-9 12:12:20', 'processTimeSecond': 99})
    infile.close()
    print(jsonObj[-1]['id'])

    with open('usage.json', 'w') as out:
        newFile = json.dump(jsonObj, out, indent=4)
        out.close()

    with gzip.open('usage.json.gz', 'wb') as out_zip:
        out_zip.write(json.dumps(jsonObj).encode('utf-8'))
        out_zip.close()

main()