from datetime import datetime, timedelta
import clickhouse_connect
import configparser

table_name = 'dsp.server1'
c = configparser.ConfigParser()
c.read('config.ini')
client1 = clickhouse_connect.get_client(host=c['Server 1']['host'], port=int(c['Server 1']['port']),
                                        username=c['Server 1']['username'], password=c['Server 1']['password'])
date = datetime(2023, 1, 1)
while date != datetime(2023, 2, 28):
	str_date_ = date.strftime('%Y-%m-%d')
	print(str_date_)
    str_date = f'toDateTime({str_date_})'
	data = client1.query(f'''
            SELECT
                {str_date}, dict_dsp_geo.region as region, dict_dsp_geo.city as city, dict_dsp_geo.id as city_id,
                month.uniques as month_unique, coalesce(week.uniques, 0) as week_unique, coalesce(day.uniques, 0) as day_unique
            FROM dict_dsp_geo
            INNER JOIN (
                SELECT geo__ip2loc__fias as city, count(distinct adv_id) as uniques FROM l
                WHERE date BETWEEN {str_date} - 30 AND {str_date} - 1
                GROUP BY geo__ip2loc__fias
                ) as month
            ON dict_dsp_geo.city = month.city
            LEFT JOIN (
                SELECT geo__ip2loc__fias as city, count(distinct adv_id) as uniques FROM l
                WHERE date BETWEEN {str_date} - 7 AND {str_date} - 1
                GROUP BY geo__ip2loc__fias
                ) as week
            ON month.city = week.city
            LEFT JOIN (
                SELECT geo__ip2loc__fias as city, count(distinct adv_id) as uniques FROM l
                WHERE date BETWEEN {str_date} - 1 AND {str_date} - 1
                GROUP BY geo__ip2loc__fias
            ) as day
            ON month.city = day.city;''').result_set
	for i in data:
            client1.command(f"""INSERT INTO {table_name} (t_stamp, date, region, city, id, month_unique, week_unique, day_unique)
                                 VALUES (toDateTime('{i[0]}'), toDateTime('{i[0]}'), '{i[1]}', '{i[2]}', '{i[3]}', '{i[6]}', '{i[5]}', '{i[4]}')""")
	date += timedelta(days=1)
