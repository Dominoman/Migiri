from sqlalchemy import select, insert, func
from tqdm import tqdm

import config
from database import Database, Base

YIELD_COUNT=1000

source=Database(config.DB_SOURCE,config.DB_DEBUG)
c_source=source.engine.connect()
destination=Database(config.DB_DESTINATION,config.DB_DEBUG)
c_destination=destination.engine.connect()

for table in ['search','itinerary','route','itinerary2route','routehistory']:
    table_object=Base.metadata.tables[table]
    count=c_source.execute(select(func.count()).select_from(table_object)).scalar()
    result=c_source.execute(select(table_object)).yield_per(YIELD_COUNT)

    insert_stmt = insert(table_object)
    for row in tqdm(result,desc=table,total=count):
        c_destination.execute(insert_stmt,row._mapping)
    c_destination.commit()

