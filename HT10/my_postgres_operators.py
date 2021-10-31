from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


from hdfs import InsecureClient
from contextlib import closing

class myPostgresOperator_TablesToCSV(BaseOperator):

    template_fields = ('sql',)
    template_ext = ('.sql',)
    ui_color = '#ededed'

    @apply_defaults
    def __init__(
            self,  folder, table, sql,
            postgres_conn_id='postgres_default', autocommit=False,
            parameters=None,
            database=None,
            *args, **kwargs):
        super(myPostgresOperator_TablesToCSV, self).__init__(*args, **kwargs)
        self.sql=sql
        self.table = table
        self.folder = folder
        self.postgres_conn_id = postgres_conn_id
        self.autocommit = autocommit
        self.parameters = parameters
        self.database = database
        self.log.info('Executing: %s', self.sql)

    def execute(self, context):
        client = InsecureClient('http://127.0.0.1:50070', user='user')
        self.hook = PostgresHook(postgres_conn_id=self.postgres_conn_id,
                                 schema=self.database)
        
        

                        
        sql = "COPY "+self.table+ " TO STDOUT WITH HEADER CSV"
        self.log.info('SQL: %s', sql)
      
    

        filename = "_"+self.table+".csv"
        with closing(self.hook.get_conn()) as conn:
            with closing(conn.cursor()) as cur:
                with client.write(self.folder+'/'+filename, overwrite=True) as csv_file:
                   cur.copy_expert(sql, csv_file)
        
   
        for output in self.hook.conn.notices:
            self.log.info(output)