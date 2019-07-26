import collections
import random
from json import load, dumps

names = []
accounts = ['twitter', 'instagram', 'facebook']
with open('dataset1.json') as json_file:
    data = load(json_file)
    for person in data:
        names.append(person['name'])

database = []
filename = 'dataset2'
count = 0

for x in range(67):
    for i in range(3):
        database.append(collections.OrderedDict([
            ('name', names[x]),
            ('account', accounts[i]),
            ('posts', random.randint(0, 25))
        ]))
del database[-1]

with open('%s.json' % filename, 'w') as output:
    output.write(dumps(database, indent=4))
print("Data set 2 Created.")