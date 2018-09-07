#!/usr/bin/env python
#
# Apache airflow utility to pause (Off state) all dags
#
# Works with mysql and postgres backend not sqlite
#
from airflow import settings
import textwrap
import os
import sys

def pause_all_dags():
    session = settings.Session()
    dag_whitelist = []
    pause_daglist = []

    cursor = session.connection().execute('SELECT dag.dag_id FROM public.dag WHERE dag.is_paused = false')
    raw_results = cursor.fetchall()
    for dag in [x[0] for x in raw_results]:
        if dag in dag_whitelist:
            continue
        pause_daglist.append(dag)

    if len(pause_daglist)==0:
        print("No DAGs need pausing")
        return

    where_list = []
    for dag in pause_daglist:
        where_list.append("dag_id = '{}'\n".format(dag))

    where_clause = 'OR '.join(where_list)

    pause_sql = textwrap.dedent("""UPDATE public.dag SET is_paused = true WHERE {}""".format(where_clause))
    
    #print(pause_sql)
    print("{} DAGs paused").format(len(pause_daglist))

    session.execute(pause_sql)
    session.commit()
    session.close()


def main():
    pause_all_dags()


if __name__ == '__main__':
    main()
