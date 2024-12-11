
import getpass
sys_user = getpass.getuser()
from jinja2 import Environment, FileSystemLoader, StrictUndefined
import pandas as pd

def execute_jinja_query(db_connector, jinja_query_path, query_parameters, return_df=False):
    if sys_user == 'ubuntu':
            env = Environment(loader=FileSystemLoader(f'/home/{sys_user}/gtp/backend/src/queries/postgres'), undefined=StrictUndefined)
    else:
            env = Environment(loader=FileSystemLoader('src/queries/postgres'), undefined=StrictUndefined)

    template = env.get_template(jinja_query_path)
    rendered_sql = template.render(query_parameters)
    print(f"...executing jinja query: {jinja_query_path} with params: {query_parameters}")
    if return_df:
        df = pd.read_sql(rendered_sql, db_connector.engine)
        return df
    else:
        db_connector.engine.execute(rendered_sql)
        return None