import clickhouse_connect
import configparser
import logging


date = 'today()'
table_name = 'dsp.server2'

script = f'''
SELECT
    {date} as date,
    dict_dsp_geo.region as region,
	dict_dsp_geo.city as city,
	dict_dsp_geo.ID as geo_id,
    coalesce(month.uniques, 0) as month_unique,
    coalesce(week.uniques, 0) as week_unique,
    coalesce(day.uniques, 0) as day_unique
FROM adstream.dict_dsp_geo

INNER JOIN
    (
    SELECT
        portal_fias_geo.fias_city as city,
        count(distinct terminal_uid) as uniques
    FROM adstream.linear
        INNER JOIN adstream.portal_fias_geo
        ON linear.city = portal_fias_geo.portal_city
    WHERE
        provider_id = 7 AND
        status in ('request', 'empty') AND
        date BETWEEN {date} - 30 AND {date} - 1
    GROUP BY portal_fias_geo.fias_city
) as month
ON dict_dsp_geo.city = month.city

LEFT JOIN (
    SELECT
        portal_fias_geo.fias_city as city,
        count(distinct terminal_uid) as uniques
    FROM adstream.linear
        INNER JOIN adstream.portal_fias_geo
        ON linear.city = portal_fias_geo.portal_city
    WHERE
        provider_id = 7 AND
        status in ('request', 'empty') AND
        date BETWEEN {date} - 7 AND {date} - 1
    GROUP BY portal_fias_geo.fias_city
) as week
ON month.city = week.city

LEFT JOIN (
    SELECT
        portal_fias_geo.fias_city as city,
        count(distinct terminal_uid) as uniques
    FROM adstream.linear
        INNER JOIN adstream.portal_fias_geo
        ON linear.city = portal_fias_geo.portal_city
    WHERE
        provider_id = 7 AND
        status in ('request', 'empty') AND
        date BETWEEN {date} - 1 AND {date} - 1
    GROUP BY portal_fias_geo.fias_city
) as day
ON month.city = day.city
'''

def main():
    logging.basicConfig(filename='log.log', filemode='w', level=logging.DEBUG)
    c = configparser.ConfigParser()
    c.read('config.ini')
    client = clickhouse_connect.get_client(host=c['Server 2']['host'], port=int(c['Server 2']['port']),
                                            username=c['Server 2']['username'], password=c['Server 2']['password'])
    data = client.query(script).result_set
    logging.info(data)
    for i in data:
        client.command(f"""INSERT INTO {table_name} (date, fias_region, fias_city, fias_id, month_unique, week_unique, day_unique)
                                 VALUES toDateTime({i[0]}), '{i[1]}', '{i[2]}', '{i[3]}', '{i[6]}', '{i[5]}', '{i[4]}')""")



if __name__ == '__main__':
    main()
