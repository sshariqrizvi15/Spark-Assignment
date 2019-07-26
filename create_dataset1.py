"""
Script to Generate data set 1
"""

import collections
import random
from json import dumps
from faker import Faker
from faker.providers import phone_number


database = []
filename = 'dataset1'
length = 100
fake = Faker()
fake.add_provider(phone_number)

# Created a list to generate a dummy data set of nested json, use fake library to generate name and phone number
for x in range(length):
    database.append(collections.OrderedDict([
        ('gender', random.choice(['M', 'F'])),
        ('name', fake.first_name() + ' ' + fake.last_name()),
        ('phone', {'mobile': fake.phone_number(), 'home': fake.phone_number()})
    ]))

# Write to a json file
with open('%s.json' % filename, 'w') as output:
    output.write(dumps(database, indent=4))
print("Data set 1 Created.")
