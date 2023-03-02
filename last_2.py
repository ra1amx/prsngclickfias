from datetime import datetime, timedelta
import clickhouse_connect
import configparser

table_name = 'dsp.server2'
c = configparser.ConfigParser()
c.read('config.ini')
client1 = clickhouse_connect.get_client(host=c['Server 1']['host'], port=int(c['Server 1']['port']),
                                        username=c['Server 1']['username'], password=c['Server 1']['password'])
client2 = clickhouse_connect.get_client(host=c['Server 2']['host'], port=int(c['Server 2']['port']),
                                        username=c['Server 2']['username'], password=c['Server 2']['password'])
date = datetime(2023, 1, 1)
while date != datetime(2023, 2, 28):
	str_date_ = date.strftime('%Y-%m-%d')
	print(str_date_)
str_date = f'toDateTime({str_date_})'
data = client2.query(f'''
            SELECT
                {str_date} as date, dict_dsp_geo.region as region, dict_dsp_geo.city as city, dict_dsp_geo.ID as geo_id,
                coalesce(month.uniques, 0) as month_unique, coalesce(week.uniques, 0) as week_unique, coalesce(day.uniques, 0) as day_unique
            FROM adstream.dict_dsp_geo
            INNER JOIN(
                SELECT portal_fias_geo.fias_city as city, count(distinct terminal_uid) as uniques
                FROM adstream.linear
                    INNER JOIN adstream.portal_fias_geo ON linear.city = portal_fias_geo.portal_city
                WHERE
                    provider_id = 7 AND status in ('request', 'empty') AND date BETWEEN {str_date} - 30 AND {str_date} - 1
                GROUP BY portal_fias_geo.fias_city
            ) as month
            ON dict_dsp_geo.city = month.city
            LEFT JOIN (
                SELECT portal_fias_geo.fias_city as city, count(distinct terminal_uid) as uniques
                FROM adstream.linear
                    INNER JOIN adstream.portal_fias_geo ON linear.city = portal_fias_geo.portal_city
                WHERE
                    provider_id = 7 AND status in ('request', 'empty') AND date BETWEEN {str_date} - 7 AND {str_date} - 1
                GROUP BY portal_fias_geo.fias_city
            ) as week
            ON month.city = week.city
            LEFT JOIN (
                SELECT portal_fias_geo.fias_city as city, count(distinct terminal_uid) as uniques
                FROM adstream.linear
                    INNER JOIN adstream.portal_fias_geo ON linear.city = portal_fias_geo.portal_city
                WHERE
                    provider_id = 7 AND status in ('request', 'empty') AND date BETWEEN {str_date} - 1 AND {str_date} - 1
                GROUP BY portal_fias_geo.fias_city
            ) as day
            ON month.city = day.city''').result_set
for i in data:
            client1.command(f"""INSERT INTO {table_name} (t_stamp, date, region, city, id, month_unique, week_unique, day_unique)
                                 VALUES (toDateTime('{i[0]}'), toDateTime('{i[0]}'), '{i[1]}', '{i[2]}', '{i[3]}', '{i[6]}', '{i[5]}', '{i[4]}')""")
date += timedelta(days=1)
