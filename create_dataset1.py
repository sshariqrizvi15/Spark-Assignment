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

for x in range(length):
    database.append(collections.OrderedDict([
        ('gender', random.choice(['M', 'F'])),
        ('name', fake.first_name() + ' ' + fake.last_name()),
        ('phone', {'mobile': fake.phone_number(), 'home': fake.phone_number()})
    ]))

with open('%s.json' % filename, 'w') as output:
    output.write(dumps(database, indent=4))
print("Data set 1 Created.")
