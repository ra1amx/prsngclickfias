import clickhouse_connect
import configparser
from dataclasses import dataclass


date = 'today()'
table1 = 't1'
table2 = 't2'
table_sum = 'dsp_new'


@dataclass
class Data:
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


def main():
    client1 = clickhouse_connect.get_client(host=c['Server 1']['host'], port=int(c['Server 1']['port']),
                                            username=c['Server 1']['username'], password=c['Server 1']['password'])
    client2 = clickhouse_connect.get_client(host=c['Server 2']['host'], port=int(c['Server 2']['port']),
                                            username=c['Server 2']['username'], password=c['Server 2']['password'])
    data1_raw = client1.query(f' SELECT (fias_region, fias_city, fias_id, month_unique, week_unique, day_unique) FROM {table1} WHERE date={date}').result_set
    data2_raw = client2.query(f' SELECT (fias_region, fias_city, fias_id, month_unique, week_unique, day_unique) FROM {table2} WHERE date={date}').result_set
    data1 = [Data(i[0], i[1], i[2], i[3], i[4], i[5]) for i in data1_raw]
    data2 = [Data(i[0], i[1], i[2], i[3], i[4], i[5]) for i in data2_raw]
    for index, value in enumerate(data1):
        for i in data2:
            if value.city == i.city:
                data1[index] += i
                break
    for i in data1:
        client1.command(f"""INSERT INTO {table_sum} (fias_region, fias_city, fias_id, month_unique, week_unique, day_unique)
                                 VALUES '{i.region}', '{i.city}', '{i.city_id}',
                                 '{i.month_unique}', '{i.week_unique}', '{i.day_unique}')""")


if __name__ == '__main__':
    main()

