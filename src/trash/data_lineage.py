import pandas as pd
import networkx as nx

# Create a DataLineage Table
df_glue = pd.read_csv('/resources/glue_meta.csv')
df_egretta = pd.read_csv('/Users/ed/writing/kcd/goo_utils/resources/jenkins_meta_info.csv')


# print(df_egretta.head(5))

df_temp = df_egretta.merge(df_glue, how='left', left_on='sink_location', right_on='location')

df_lineage = df_temp[['job_name','source_table','sink_location','db_name','table_name']]
df_lineage.columns = ['job_name','source_tb_name','sink_location','sink_db_name','sink_tb_name']
df_lineage.sort_values('sink_location')
df_lineage.to_csv('data_lineage.csv',index=False)

# Create a DataLineage graph

def create_subgraph(g,sub_g,start_node):
    for n in g.predecessors(start_node):
        sub_g.add_edge(n,start_node)
        create_subgraph(g,sub_g,n)

G = nx.DiGraph()

