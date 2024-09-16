from sqlalchemy import select, insert
from tqdm import tqdm

import config
from database import Database, Base

source=Database(config.DB_SOURCE,config.DB_DEBUG)
c_source=source.engine.connect()
destination=Database(config.DB_DESTINATION,config.DB_DEBUG)
c_destination=destination.engine.connect()

for table in ['search','itinerary','route','itinerary2route','routehistory']:
    result=c_source.execute(select(Base.metadata.tables[table])).all()

    insert_stmt = insert(Base.metadata.tables[table])
    for row in tqdm(result,desc=table):
        c_destination.execute(insert_stmt,row._mapping)
    c_destination.commit()

