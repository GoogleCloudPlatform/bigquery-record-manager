# Copyright 2022 Google, LLC.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from google.cloud import firestore
import uuid
import networkx as nx

import Utils as ut

db = firestore.Client()

class GraphService:
    
    @staticmethod
    def init_graph():
       
       print('INFO: constructing graph')
       
       G = nx.Graph()
       
       fks = ut.Utils.get_foreign_keys() 
    
       for fk in fks:
           
           #print('fk: ' + str(fk))
           
           pk_table = fk['pk_table']
           fk_table = fk['fk_table']
           pk_columns = fk['pk_columns']
           fk_columns = fk['fk_columns']
           
           if pk_table not in G:
               
               pk_group = ut.Utils.lookup_group(pk_table)
               
               if pk_group:
                   G.add_node(pk_table)
                   G.nodes[pk_table]['group'] = pk_group
                   print('INFO: added node: ' + pk_table + ' with group ' + pk_group)
               #else:            
                   #print("didn't add node: " + pk_table + " because it has no group")
             
           if fk_table not in G:
               
               fk_group = ut.Utils.lookup_group(fk_table)
               
               if fk_group:
                   G.add_node(fk_table)
                   G.nodes[fk_table]['group'] = fk_group
                   print('INFO: added node: ' + fk_table + ' with group ' + fk_group)
               #else:            
                   #print("didn't add node: " + fk_table + " because it has no group")
        
           # create edge and attach attributes named after the pk_table and fk_table
           if pk_table in G and fk_table in G:
               G.add_edge(pk_table, fk_table)
               G[pk_table][fk_table][pk_table] = pk_columns
               G[pk_table][fk_table][fk_table] = fk_columns
           
               print('INFO: added edge between ' + pk_table + ' and ' + fk_table)
       
       print('INFO: completed graph')
           
       return G        

    
    @staticmethod
    def get_neighbors(G, entity_path):
       
       neighbors = list(G.neighbors(entity_path))
       return neighbors

    
if __name__ == '__main__':
    gs = GraphService();
    G = gs.init_graph()
    neighbors = gs.get_neighbors(G, 'ridb.Orders')
    print("ridb.Order's neighbors: " + str(neighbors))
    
    