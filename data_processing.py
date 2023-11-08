import csv, os

__location__ = os.path.realpath(
    os.path.join(os.getcwd(), os.path.dirname(__file__)))

cities = []
with open(os.path.join(__location__, 'Cities.csv')) as f:
    rows = csv.DictReader(f)
    for r in rows:
        cities.append(dict(r))

countries = []
with open(os.path.join(__location__, 'Countries.csv')) as f:
    rows = csv.DictReader(f)
    for r in rows:
        countries.append(dict(r))

player = []
with open(os.path.join(__location__, 'Players.csv')) as f:
    rows = csv.DictReader(f)
    for r in rows:
        player.append(dict(r))

team = []
with open(os.path.join(__location__, 'Teams.csv')) as f:
    rows = csv.DictReader(f)
    for r in rows:
        team.append(dict(r))

titanic = []
with open(os.path.join(__location__, 'Titanic.csv')) as f:
    rows = csv.DictReader(f)
    for r in rows:
        titanic.append(dict(r))

class DB:
    def __init__(self):
        self.database = []

    def insert(self, table):
        self.database.append(table)

    def search(self, table_name):
        for table in self.database:
            if table.table_name == table_name:
                return table
        return None
    
import copy
class Table:
    def __init__(self, table_name, table):
        self.table_name = table_name
        self.table = table
    
    def join(self, other_table, common_key):
        joined_table = Table(self.table_name + '_joins_' + other_table.table_name, [])
        for item1 in self.table:
            for item2 in other_table.table:
                if item1[common_key] == item2[common_key]:
                    dict1 = copy.deepcopy(item1)
                    dict2 = copy.deepcopy(item2)
                    dict1.update(dict2)
                    joined_table.table.append(dict1)
        return joined_table
    
    def filter(self, condition):
        filtered_table = Table(self.table_name + '_filtered', [])
        for item1 in self.table:
            if condition(item1):
                filtered_table.table.append(item1)
        return filtered_table
    
    def aggregate(self, function, aggregation_key):
        temps = []
        for item1 in self.table:
            temps.append(float(item1[aggregation_key]))
        return function(temps)
    
    def select(self, attributes_list):
        temps = []
        for item1 in self.table:
            dict_temp = {}
            for key in item1:
                if key in attributes_list:
                    dict_temp[key] = item1[key]
            temps.append(dict_temp)
        return temps

    def __str__(self):
        return self.table_name + ':' + str(self.table)


table1 = Table('cities', cities)
table2 = Table('countries', countries)
table3 = Table('player', player)
table4 = Table('team', team)
table5 = Table('titanic', titanic)
my_DB = DB()
my_DB.insert(table1)
my_DB.insert(table2)
my_DB.insert(table3)
my_DB.insert(table4)
my_DB.insert(table5)
my_table1 = my_DB.search('cities')
my_table3 = my_DB.search('player')
my_table3_filtered = my_table3.filter(lambda x: 'ia' in x['team']).filter(lambda x: int((x['minutes'])) < 200)\
    .filter(lambda x: int((x['passes'])) > 100)
my_table3_selected = my_table3_filtered.select(['surname', 'team', 'position'])
print(my_table3_selected)
my_table4 = my_DB.search('team')
avg_below = my_table4.filter(lambda x: int(x['ranking']) < 10).aggregate(lambda x: sum(x)/len(x),'games')
avg_higher = my_table4.filter(lambda x: int(x['ranking']) >= 10).aggregate(lambda x: sum(x)/len(x),'games')

print(f'avg ranking Below 10 : {avg_below:.2f} vs ranking above or equal 10 :{avg_higher:.2f}')

avg_forward = my_table3.filter(lambda x: (x['position']) == 'forward').aggregate(lambda x: sum(x)/len(x),'passes')
avg_midfielder = my_table3.filter(lambda x: (x['position']) == 'midfielder').aggregate(lambda x: sum(x)/len(x),'passes')
print(f'avg forward :{avg_forward:.2f}')
print(f'avg midfielder: {avg_midfielder:.2f}')

my_table5 = my_DB.search('titanic')
avg_first = my_table5.filter(lambda x: (x['class']) == '1').aggregate(lambda x: sum(x)/len(x),'fare')
avg_third = my_table5.filter(lambda x: (x['class']) == '3').aggregate(lambda x: sum(x)/len(x),'fare')
print(f'Frist Class Fare : {avg_first:.2f} vs Third Class Fare : {avg_third:.2f}')

male_survival = my_table5.filter(lambda x: (x['gender']) == 'M').filter(lambda x: (x['survived']) == 'yes')
all_male = my_table5.filter(lambda x: (x['gender']) == 'M')
male_survival_rate = len(male_survival.table)/len(all_male.table)
female_survival = my_table5.filter(lambda x: (x['gender']) == 'F').filter(lambda x: (x['survived']) == 'yes')
all_female = my_table5.filter(lambda x: (x['gender']) == 'F')
female_survival_rate = len(female_survival.table)/len(all_female.table)
print(f' male rate : {male_survival_rate:.2f} vs female rate: {female_survival_rate:.2f}')
# print(my_table3.table_name, my_table3.table)

# print("Test filter: only filtering out cities in Italy")
# my_table1_filtered = my_table1.filter(lambda x: x['country'] == 'Italy')
# print(my_table1_filtered)
# print()
#
# print("Test select: only displaying two fields, city and latitude, for cities in Italy")
# my_table1_selected = my_table1_filtered.select(['city', 'latitude'])
# print(my_table1_selected)
# print()
#
# print("Calculting the average temperature without using aggregate for cities in Italy")
# temps = []
# for item in my_table1_filtered.table:
#     temps.append(float(item['temperature']))
# print(sum(temps)/len(temps))
# print()
#
# print("Calculting the average temperature using aggregate for cities in Italy")
# print(my_table1_filtered.aggregate(lambda x: sum(x)/len(x), 'temperature'))
# print()
#
# print("Test join: finding cities in non-EU countries whose temperatures are below 5.0")
# my_table2 = my_DB.search('countries')
# my_table3 = my_table1.join(my_table2, 'country')
# my_table3_filtered = my_table3.filter(lambda x: x['EU'] == 'no').filter(lambda x: float(x['temperature']) < 5.0)
# print(my_table3_filtered.table)
# print()
# print("Selecting just three fields, city, country, and temperature")
# print(my_table3_filtered.select(['city', 'country', 'temperature']))
# print()
#
# print("Print the min and max temperatures for cities in EU that do not have coastlines")
# my_table3_filtered = my_table3.filter(lambda x: x['EU'] == 'yes').filter(lambda x: x['coastline'] == 'no')
# print("Min temp:", my_table3_filtered.aggregate(lambda x: min(x), 'temperature'))
# print("Max temp:", my_table3_filtered.aggregate(lambda x: max(x), 'temperature'))
# print()

# print("Print the min and max latitude for cities in every country")
# for item in my_table2.table:
#     my_table1_filtered = my_table1.filter(lambda x: x['country'] == item['country'])
#     if len(my_table1_filtered.table) >= 1:
#         print(item['country'], my_table1_filtered.aggregate(lambda x: min(x), 'latitude'), my_table1_filtered.aggregate(lambda x: max(x), 'latitude'))
# print()

