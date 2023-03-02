import clickhouse_connect
import configparser
import logging


date = 'today()'
table_name = 'dsp.server1'

script = f'''
SELECT
    {date},
    dict_dsp_geo.region as region,
    dict_dsp_geo.city as city,
    dict_dsp_geo.id as city_id,
    month.uniques as month_unique,
    coalesce(week.uniques, 0) as week_unique,
    coalesce(day.uniques, 0) as day_unique
FROM dict_dsp_geo -- Справочник ГЕО_ФИАС
INNER JOIN (
    SELECT
        geo__ip2loc__fias as city,
        count(distinct adv_id) as uniques
    FROM l
    WHERE date BETWEEN {date} - 30 AND {date} - 1
    GROUP BY geo__ip2loc__fias
    ) as month
ON dict_dsp_geo.city = month.city
LEFT JOIN (
    SELECT
        geo__ip2loc__fias as city,
        count(distinct adv_id) as uniques
    FROM l
    WHERE date BETWEEN {date} - 7 AND {date} - 1
    GROUP BY geo__ip2loc__fias
    ) as week
ON month.city = week.city
LEFT JOIN (
    SELECT
        geo__ip2loc__fias as city,
        count(distinct adv_id) as uniques
    FROM l
    WHERE date BETWEEN {date} - 1 AND {date} - 1
    GROUP BY geo__ip2loc__fias
) as day
ON month.city = day.city;
'''

def main():
    logging.basicConfig(filename='log.log', filemode='w', level=logging.DEBUG)
    c = configparser.ConfigParser()
    c.read('config.ini')
    client = clickhouse_connect.get_client(host=c['Server 1']['host'], port=int(c['Server 1']['port']),
                                            username=c['Server 1']['username'], password=c['Server 1']['password'])
    data = client.query(script).result_set
    logging.info(data)
    for i in data:
        client.command(f"""INSERT INTO {table_name} (date, fias_region, fias_city, fias_id, month_unique, week_unique, day_unique)
                                 VALUES toDateTime({i[0]}), '{i[1]}', '{i[2]}', '{i[3]}', '{i[6]}', '{i[5]}', '{i[4]}')""")



if __name__ == '__main__':
    main()
