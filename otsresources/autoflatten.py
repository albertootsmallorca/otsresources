from collections import Counter
import json
from pyspark.sql.functions import lit, col, when, explode, explode_outer, length, max
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format
from pyspark.sql import functions as F, Window


class AutoFlatten:
    def __init__(self, json_schema):
        self.fields_in_json = self.get_fields_in_json(json_schema)
        self.all_fields = {}
        self.cols_to_explode = set()
        self.structure = {}
        self.order = []
        self.bottom_to_top = {}
        self.rest = set()
        
        
    def get_fields_in_json(self, json_schema):
        '''
        Description: 
        This function takes in the schema in json format and returns the metadata of the schema
        :param json_schema: [type : str] a string containing path to raw data
        :return fields: [type : dict] contains metadata of the schema
        '''
        a = json_schema.json()
        schema_dict = json.loads(a)
        fields = schema_dict['fields']
        return fields
        
        
    def is_leaf(self, data):
        '''
        Description: 
        This function checks if the particular field in the schema is a leaf or not.
        Types not considered as a leaf : struct, array
        :param data: [type: dict] a dictionary containing metadata about a field
        :return leaf: [type: bool] indicates whether a given field is a leaf or not
        '''
        try:
            if isinstance(data['type'], str):
                leaf = True if data['type'] != 'struct' else False
            else:
                leaf = True if data['type']['type'] == 'map' else False
        except:
            leaf = False
        finally:
            return leaf
            
            
    def unnest_dict(self, json, cur_path):
        '''
        Description: 
        This function unnests the dictionaries in the json schema recursively 
        and maps the hierarchical path to the field to the column name when it encounters a leaf node
        :param json: [type: dict/list] contains metadata about a field
        :param cur_path: [type: str] contains hierarchical path to that field, each parent separated by a '.'
        '''
        if self.is_leaf(json):
            self.all_fields[f"{cur_path}.{json['name']}"] = json['name']
            return
        else:
            if isinstance(json, list):
                for i in range(len(json)):
                    self.unnest_dict(json[i], cur_path)
            elif isinstance(json, dict):
                if isinstance(json['type'], str):
                    cur_path = f"{cur_path}.{json['name']}" if json['type'] != 'struct' else cur_path
                    self.unnest_dict(json['type'], cur_path)
                else:
                    if json['type']['type'] == 'array':
                        cur_path = f"{cur_path}.{json['name']}"
                        if isinstance(json['type']['elementType'], dict):
                            self.cols_to_explode.add(cur_path)
                            self.unnest_dict(json['type']['elementType']['fields'], cur_path)
                        else:
                            self.cols_to_explode.add(cur_path)
                            self.all_fields[f"{cur_path}"] = json['name']
                            return
                    elif json['type']['type'] == 'struct':
                        cur_path = f"{cur_path}.{json['name']}"
                        self.unnest_dict(json['type']['fields'], cur_path)
                        
                        
    def get_structure(self, col_list):
        '''
        Description: 
        This function gets the structure to the traversal to array field in the schema
        :param col_list: [type: list] contains list of fields that are to be exploded
        :return structure: [type: dict] contains the hierarchical mapping for array fields
        '''
        structure = {'json' : {}}
        for val in col_list:
            arr = val.split('.')
            a = structure['json']
            for i in range(1,len(arr)):
                if not a.__contains__(arr[i]):
                    a[arr[i]] = {} 
                a = a[arr[i]]
        return structure
        
        
    
    def extract_order(self, structure):
        '''
        Description: 
        This function does a BFS traversal to obtain the order in which 
        the array type fields are to be exploded
        :param structure: [type: dict] contains the hierarchical mapping for array fields
        :return order: [type: list] contains the fields in order in which array explode has to take place
        '''
        q = [('', structure['json'])]
        order = []
        while q:
            key, a = q.pop(0)
            for x in a.keys():
                order.append(f"{key}.{x}")
                q.append((f"{key}.{x}", a[x]))
        return order
        
        
        
    def get_bottom_to_top(self, order, all_cols_in_explode_cols):
        '''
        Description: 
        This function gets the mutually exclusive leaf fields in every array type column
        :param order: [type: list] contains the fields in order in which array explode has to take place
        :param all_cols_in_explode_cols: [type: set] contains all fields in array type fields
        :return bottom_to_top: [type: dict] contains list of mutually exclusive leaf fields for every 
                                array type / struct type (parent to array type) field
        '''
        bottom_to_top = {}
        for column in reversed(order):
            #x_cols = set(filter(lambda x: x.startswith(column), list(all_cols_in_explode_cols)))
            x_cols = set(x for x in list(all_cols_in_explode_cols) if x.startswith(column))
            bottom_to_top[column] = list(x_cols)
            all_cols_in_explode_cols = all_cols_in_explode_cols.difference(x_cols)
        return bottom_to_top
        
        
        
    def compute(self):
        '''
        Description: 
        This function performs the required computation and gets all the resources 
        needed for further process of selecting and exploding fields
        '''
        self.unnest_dict(self.fields_in_json, '')
        #print('self.cols_to_explode', self.cols_to_explode) # cambios
        #print('self.all_fields.keys()', self.all_fields.keys()) # cambios
        #all_cols_in_explode_cols = set(filter(lambda x: x.startswith(tuple(self.cols_to_explode)), self.all_fields.keys()))
        all_cols_in_explode_cols = set(x for x in self.all_fields.keys() if x.startswith(tuple( self.cols_to_explode)))
        
        # self.all_fields.keys().values() ???
        
        
        #print('all_cols_in_explode_cols',all_cols_in_explode_cols) # cambios
        self.rest = set(self.all_fields.keys()).difference(all_cols_in_explode_cols)
        self.structure = self.get_structure([f"json{x}" for x in list(self.cols_to_explode)])
        self.order = self.extract_order(self.structure)
        self.bottom_to_top = self.get_bottom_to_top(self.order, all_cols_in_explode_cols)
        

def column_separator(df1,af):
    visited = set([f'.{column}' for column in df1.columns])
    duplicate_target_counter = Counter(af.all_fields.values())
    cols_to_select = df1.columns
    for rest_col in af.rest:
        if rest_col not in visited:
            cols_to_select += [rest_col[1:]] if (duplicate_target_counter[af.all_fields[rest_col]]==1 and af.all_fields[rest_col] not in df1.columns) else [col(rest_col[1:]).alias(f"{rest_col[1:].replace('.', '_')}")]
            visited.add(rest_col)

    df1 = df1.select(cols_to_select)


    if af.order:
        for key in af.order:
            column = key.split('.')[-1]
            if af.bottom_to_top[key]:
                #########
                #values for the column in bottom_to_top dict exists if it is an array type
                #########
                df1 = df1.select('*', explode_outer(col(column)).alias(f"{column}_exploded")).drop(column)
                data_type = df1.select(f"{column}_exploded").schema.fields[0].dataType
                if not (isinstance(data_type, StructType) or isinstance(data_type, ArrayType)):
                    df1 = df1.withColumnRenamed(f"{column}_exploded", column if duplicate_target_counter[af.all_fields[key]]<=1 else key[1:].replace('.', '_'))
                    visited.add(key)
                else:
                    #grabbing all paths to columns after explode
                    cols_in_array_col = set(map(lambda x: f'{key}.{x}', df1.select(f'{column}_exploded.*').columns))
                    #retrieving unvisited columns
                    cols_to_select_set = cols_in_array_col.difference(visited)
                    all_cols_to_select_set = set(af.bottom_to_top[key])
                    #check done for duplicate column name & path
                    cols_to_select_list = list(map(lambda x: f"{column}_exploded{'.'.join(x.split(key)[1:])}" if (duplicate_target_counter[af.all_fields[x]]<=1 and x.split('.')[-1] not in df1.columns) else col(f"{column}_exploded{'.'.join(x.split(key)[1:])}").alias(f"{x[1:].replace('.', '_')}"), list(all_cols_to_select_set)))
                    #updating visited set
                    visited.update(cols_to_select_set)
                    rem = list(map(lambda x: f"{column}_exploded{'.'.join(x.split(key)[1:])}", list(cols_to_select_set.difference(all_cols_to_select_set))))
                    df1 = df1.select(df1.columns + cols_to_select_list + rem).drop(f"{column}_exploded")        
            else:
                #########
                #values for the column in bottom_to_top dict do not exist if it is a struct type / array type containing a string type
                #########
                #grabbing all paths to columns after opening
                cols_in_array_col = set(map(lambda x: f'{key}.{x}', df1.selectExpr(f'{column}.*').columns))
                #retrieving unvisited columns
                cols_to_select_set = cols_in_array_col.difference(visited)
                #check done for duplicate column name & path
                cols_to_select_list = list(map(lambda x: f"{column}.{x.split('.')[-1]}" if (duplicate_target_counter[x.split('.')[-1]]<=1 and x.split('.')[-1] not in df1.columns) else col(f"{column}.{x.split('.')[-1]}").alias(f"{x[1:].replace('.', '_')}"), list(cols_to_select_set)))
                #updating visited set
                visited.update(cols_to_select_set)
                df1 = df1.select(df1.columns + cols_to_select_list).drop(f"{column}")


    final_df = df1.select([field[1:].replace('.', '_') if duplicate_target_counter[af.all_fields[field]]>1 else af.all_fields[field] for field in af.all_fields])
    return final_df