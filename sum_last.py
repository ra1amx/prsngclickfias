from datetime import datetime, timedelta
import clickhouse_connect
import configparser
from dataclasses import dataclass


table1 = 'dsp.server1'
table2 = 'dsp.server2'
table_sum = 'dsp_new'

@dataclass
class Data:
    t_stamp: str
    date: str
    region: str
    city: str
    city_id: int
    month_unique: int
    week_unique: int
    day_unique: int
    def __add__(self, other):
        self.month_unique += other.month_unique
        self.week_unique += other.week_unique
        self.day_unique += other.day_unique        


c = configparser.ConfigParser()
c.read('config.ini')
client1 = clickhouse_connect.get_client(host=c['Server 1']['host'], port=int(c['Server 1']['port']),
                                        username=c['Server 1']['username'], password=c['Server 1']['password'])
client2 = clickhouse_connect.get_client(host=c['Server 2']['host'], port=int(c['Server 2']['port']),
                                            username=c['Server 2']['username'], password=c['Server 2']['password'])
date = datetime(2023, 1, 1)
while date != datetime(2023, 2, 28):
	str_date = date.strftime('%Y-%m-%d')
	print(str_date)
	data1_raw = client1.query(f' SELECT * FROM {table1} WHERE date={toDateTime("str_date")}').result_set
        data2_raw = client2.query(f' SELECT * FROM {table2} WHERE date={toDateTime("str_date")}').result_set
        data1 = [Data(i[0], i[1], i[2], i[3], i[4], i[5], i[6], i[7]) for i in data1_raw]
        data2 = [Data(i[0], i[1], i[2], i[3], i[4], i[5], i[6], i[7]) for i in data2_raw]
	for i in data:
            client1.command(f"""INSERT INTO {table_sum} (t_stamp, date, fias_region, fias_city, fias_id, month_unique, week_unique, day_unique)
                                 VALUES (toDateTime('{i.t_stamp}'), toDateTime('{i.date}'), '{i.fias_region}', '{i.fias_city}',
                                 '{i.fias_id}', '{i.month_unique}', '{i.week_unique}', '{i.day_unique}')""")
	date += timedelta(days=1)
