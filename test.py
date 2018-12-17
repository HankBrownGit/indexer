from data_structures import flat_sql_schema
print('SELECT ("{}") FROM CrawlerTable'.format('", "'.join(
flat_sql_schema)))