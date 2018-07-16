from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

class Customer():
  """
  Customer Data in iDNA
  """
  def __init__(self):
    self.env = "prd"
    self.layer = "published_standard"
    self.domain = "customer"
    self.exclude_db_patterns = ["ichub", "cdl", "field", "bai"]
    self.exclude_table_patterns = ["cross_reference"]
    #self.spark = SparkSession(sc)
    self.table_ref_map = self._table_ref_map()
    
    
  def _exclude_pattern(self,mylist,exclude_pattern_list):
    """
    method type: internal
    helper method to exclude patterns
    """
    item_for_removal = []
    for item in mylist:
      for pattern in exclude_pattern_list:
        if pattern in item:
          item_for_removal.append(item)
    mylist = list(set(mylist) - set(item_for_removal))
    return mylist
  
  def _getcommonletters(self,strlist):
      """
      method type: internal
      helper method for _findcommonstart
      """
      return ''.join([x[0] for x in zip(*strlist) \
                       if reduce(lambda a,b:(a == b) and a or None,x)])

  def _findcommonstart(self,strlist):
      """
      method type: internal
      helper method for table prefix
      """
      strlist = strlist[:]
      prev = None
      while True:
          common = self._getcommonletters(strlist)
          if common == prev:
              break
          strlist.append(common)
          prev = common

      return self._getcommonletters(strlist)
  
  def _table_ref_map(self):
    """
    method type: internal
    helper method to initalize a table reference map
    """
    # fetch matching databases
    db_list = []
    df = spark.sql("show databases")
    for db in df.select("databaseName").collect():
      if self.domain in db["databaseName"] and self.layer in db["databaseName"] and self.env in db["databaseName"]:
        db_list.append(db["databaseName"])
    
    # exclude db patterns
    db_list = self._exclude_pattern(db_list,self.exclude_db_patterns)
    
    # fetch matching tables
    db_table_list = []
    for db in db_list:
      df = spark.sql("show tables from %s" %db)
      tables = []
      for table in df.select("tableName").collect():
        db_table = db + "." + table["tableName"]
        db_table_list.append(db_table)
    
    # exclude table patterns
    db_table_list = self._exclude_pattern(db_table_list,self.exclude_table_patterns)
    
    prefix = self._findcommonstart(db_table_list)
    table_ref_map = {}
    
    
    for table_name in db_table_list:
      new_table_ref = table_name.replace(prefix, '')
      table_ref_map[new_table_ref] = table_name
        
    return table_ref_map
  
  def get_data_objects(self):
    """
    method type: public
    returns list of data objects available for this class
    """
    return self.table_ref_map.keys()
  
  
  def get_data_frame(self,table_name):
    #sql_query = "select * from ( select *, dense_rank() over (order by cast(cdl_run_identifier as int) desc) cdl_run_rank  from %s where cdl_effective_date = '2018-07-15') where cdl_run_rank = 1" %(table_name)
    sql_query = "select * from delta_test_customer.s_customer_basic_nominal where cdl_effective_date = '2018-07-15'"
    df = spark.sql(sql_query)
    return df
      
  def __getattr__(self, name):
    def method(*args):
      print("handling dynamic method " + name)
      if name in self.table_ref_map.keys():
        print "method exists"
        table_name = self.table_ref_map[name]
        return self.get_data_frame(table_name)
      else:
        return "method does not exist"
    return method
